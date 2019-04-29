package com.kmjy.DBUtils
import java.sql.{Date, Timestamp}
import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types._

import scala.util.matching.Regex

object OperatorMySql {

   private val configFactory =  ConfigFactory.load()
   private val mysqlConfig = configFactory.getConfig("mysql")

   /**
     * 以元祖的额方式返回mysql属性信息
     *
     * @return
     */
   def getMysqlInfo: (String, String, String) = {
      val jdbcURL = mysqlConfig.getString("url")
      val userName = mysqlConfig.getString("username")
      val password = mysqlConfig.getString("password")
      (jdbcURL, userName, password)
   }

   def getDFFromeMysql(sqlContext: SQLContext, mysqlTableName: String, queryCondition: String = ""): DataFrame = {
      val (jdbcURL, userName, password) = getMysqlInfo
      val prop = new Properties()
      prop.put("user", userName)
      prop.put("password", password)
      //scala中其实equals和==是相同的，并不跟java中一样
      if (null == queryCondition || "" == queryCondition) {
         sqlContext.read.jdbc(jdbcURL, mysqlTableName, prop)
      } else {
         sqlContext.read.jdbc(jdbcURL, mysqlTableName, prop).where(queryCondition)
      }
   }


   /**
     *
     * @param tableName 表名
     * @param resultDateFrame 要入库的dataframe
     * @param updateColumns 要更新的字段
     * @return
     */
   def insertOrUpdateDFtoDBUserPool(tableName: String, resultDateFrame: DataFrame, updateColumns: Array[String]):Boolean = {
      var status = true
      var count = 0
      val colNumbsers = resultDateFrame.columns.length
      val sql = getInsertOrUpdateSql(tableName, resultDateFrame.columns, updateColumns)
      val columnDataTypes = resultDateFrame.schema.fields.map(_.dataType)
      println(s"\n$sql")
      resultDateFrame.foreachPartition(partitionRecords => {
         val conn = MySqlPoolManager.getMysqlManager.getConnection
         val prepareStatement = conn.prepareStatement(sql)
         val metaData = conn.getMetaData.getColumns(null, "%", tableName, "%")
         try {
            conn.setAutoCommit(false)
            partitionRecords.foreach(record => {
               //设置需要插入的字段
               for (i <- 1 to colNumbsers) {
                  val value = record.get(i - 1)
                  val dateType = columnDataTypes(i - 1)
                  if (value != null) {
                     prepareStatement.setString(i, value.toString)
                     dateType match {
                        case _: ByteType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                        case _: ShortType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                        case _: IntegerType => prepareStatement.setInt(i, record.getAs[Int](i - 1))
                        case _: LongType => prepareStatement.setLong(i, record.getAs[Long](i - 1))
                        case _: BooleanType => prepareStatement.setBoolean(i, record.getAs[Boolean](i - 1))
                        case _: FloatType => prepareStatement.setFloat(i, record.getAs[Float](i - 1))
                        case _: DoubleType => prepareStatement.setDouble(i, record.getAs[Double](i - 1))
                        case _: StringType => prepareStatement.setString(i, record.getAs[String](i - 1))
                        case _: TimestampType => prepareStatement.setTimestamp(i, record.getAs[Timestamp](i - 1))
                        case _: DateType => prepareStatement.setDate(i, record.getAs[Date](i - 1))
                        case _ => throw new RuntimeException("nonsupport $ {dateType} !!!")
                     }
                  } else {
                     metaData.absolute(i)
                     prepareStatement.setNull(i, metaData.getInt("Data_Type"))
                  }
               }
               //设置需要 更新的字段值
               for (i <- 1 to updateColumns.length) {
                  val fieldIndex = record.fieldIndex(updateColumns(i - 1))
                  val value = record.get(i)
                  val dataType = columnDataTypes(fieldIndex)
                  if (value != null) {
                     dataType match {
                        case _: ByteType => prepareStatement.setInt(colNumbsers + i, record.getAs[Int](fieldIndex))
                        case _: ShortType => prepareStatement.setInt(colNumbsers + i, record.getAs[Int](fieldIndex))
                        case _: IntegerType => prepareStatement.setInt(colNumbsers + i, record.getAs[Int](fieldIndex))
                        case _: LongType => prepareStatement.setLong(colNumbsers + i, record.getAs[Long](fieldIndex))
                        case _: BooleanType => prepareStatement.setBoolean(colNumbsers + i, record.getAs[Boolean](fieldIndex))
                        case _: FloatType => prepareStatement.setFloat(colNumbsers + i, record.getAs[Float](fieldIndex))
                        case _: DoubleType => prepareStatement.setDouble(colNumbsers + i, record.getAs[Double](fieldIndex))
                        case _: StringType => prepareStatement.setString(colNumbsers + i, record.getAs[String](fieldIndex))
                        case _: TimestampType => prepareStatement.setTimestamp(colNumbsers + i, record.getAs[Timestamp](fieldIndex))
                        case _: DateType => prepareStatement.setDate(colNumbsers + i, record.getAs[Date](fieldIndex))
                        case _ => throw new RuntimeException(s"no support ${dataType} !!!")
                     }
                  } else {
                     metaData.absolute(colNumbsers + i)
                     prepareStatement.setNull(colNumbsers + i, metaData.getInt("data_Type"))
                  }
               }
               prepareStatement.addBatch()
               count += 1
            })
            //批次大小为100
            if (count % 100 == 0) {
               prepareStatement.executeBatch()
            }
            conn.commit()
         } catch {
            case e: Exception =>
               println(s"@@  ${e.getMessage}")
               status = false
         } finally {
            prepareStatement.executeBatch()
            conn.commit()
            prepareStatement.close()
            conn.close()
         }
      })
      status
   }


   /**
     * 拼接insertOrUpdate语句
     * @param tableName 表名
     * @param cols 列
     * @param updateColumns 需要更新的列
     * @return
     */
   def getInsertOrUpdateSql(tableName: String, cols: Array[String], updateColumns: Array[String]): String = {
      val colNumbers = cols.length
      var sqlStr = "insert into " + tableName + "("
      for (i <- 1 to colNumbers) {
         sqlStr += "`"+cols(i - 1)+"`"
         if (i != colNumbers) {
            sqlStr += ","
         }
      }
      sqlStr += ") values("
      for (i <- 1 to colNumbers) {
         sqlStr += "?"
         if (i != colNumbers) {
            sqlStr += ","
         }
      }
      sqlStr += ") on duplicate key update "
      updateColumns.foreach(str => {
         sqlStr += s"`$str`=?,"
      })
      sqlStr.substring(0, sqlStr.length - 1)
   }

}

object OperatorMySqlApp {
   def main(args: Array[String]): Unit = {
      val sql = OperatorMySql.getInsertOrUpdateSql("jy_jbxx","a,b,c".split(","),"a,b".split(","))
      println(sql)
      val s="（2004.09--2008.01在中央广播电视大学本科法学专业学习）"
      val pattren = new Regex("\\d{4}.\\d{2}-.*\\d{4}.\\d{2}")
      val result = pattren.findFirstIn(s)
      result match {
         case Some(x) => {
            println(x)
         }
         case _ => null
      }
   }
}