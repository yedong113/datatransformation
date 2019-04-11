package com.kmjy.spark

import java.sql.{Connection, DriverManager}
import java.util.{Date, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
object Police {

   private val configFactory =  ConfigFactory.load()
   private val mySqlConfig = configFactory.getConfig("mysql")
   private val policeMySqlConfig = configFactory.getConfig("police_mysql")
   private val dwMySqlConfig = configFactory.getConfig("dw_mysql")
   val fileConfig = configFactory.getConfig("file")
   val rootpath = fileConfig.getString("rootpath")
   val url = policeMySqlConfig.getString("url")
   println(rootpath+fileConfig.getString("A01"))
   val properties = new Properties()
   properties.setProperty("user",mySqlConfig.getString("username"))
   properties.setProperty("password",mySqlConfig.getString("password"))
   //properties.setProperty("driver",mySqlConfig.getString("driver"))
   properties.setProperty("url",policeMySqlConfig.getString("url"))

   private val sparkConf = new SparkConf().setMaster("local[*]")
         .setAppName("kmjy datatransformation")

   private val spark = SparkSession.builder().config(sparkConf).getOrCreate()

   private val sc = spark.sparkContext

   private val policeRDD = sc.textFile(rootpath+fileConfig.getString("A01"))
   private val sfzRDD = sc.textFile(rootpath+fileConfig.getString("sfzhjh"))
   private val mdictRDD = sc.textFile(rootpath+fileConfig.getString("mdict"))
   private val dwRDD = sc.textFile(rootpath+fileConfig.getString("B01"))

   val headerPolice = policeRDD.first()
   val policeFullRDD = policeRDD.filter(_!=headerPolice).map(_.split(","))
   val policeRDDRows = policeFullRDD
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),
            line(5),line(7),line(8),line(9),line(11),
            line(12),line(16),line(18),line(20),
            {
               import java.text.SimpleDateFormat
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            }
         ))

   val policeJianLi = policeFullRDD.map(line=>Row(line(0),line(1),line(28)))

   private val dwRDDRows = dwRDD.map(_.split(",")).map(line=>Row(
      line(0),line(1),line(2),line(3),line(4),
      line(5),line(6),
      line(8),line(9),
      line(15),line(16),line(17),line(18)
   ))
   private val dwDataFrame = spark.createDataFrame(dwRDDRows,
      StructType("JGMC,JGJC,JGBM,SZZQ,".split(",").map(column=>StructField(column,StringType,true))))


   //数据字典
   private val header = mdictRDD.first()
   private val mdictRDDRows = mdictRDD.filter(_!=header)//.map(_.split(",",-1))
         .map(line=>line.split(";")).map(line=>Row(line(5),line(6),line(7)))
   private val mdictDataFrame:DataFrame = spark.createDataFrame(mdictRDDRows,
      StructType("tree_alias,tree_label,tree_value".split(",").map(column=>StructField(column,StringType,true))))

   //身份证信息
   private val header2 = sfzRDD.first()
   private val sfzRDDRows = sfzRDD.filter(_!=header2).map(_.split(","))
         .map(line=>Row(line(0),line(1))).cache()
   private val sfzDataFrame:DataFrame = spark.createDataFrame(sfzRDDRows,
      StructType("JMSFHM,JH".split(",").map(column=>StructField(column,StringType,true))))


   val policeDataFrame = spark.createDataFrame(policeRDDRows,
      StructType("XM,JMSFHM,XB_AI,CSRQ,JG_AI,CSD_AI,MZ_AI,JKZK_AI,CJGZSJ,ZZMM_AI,CJZZRQ,RYGLZT_AI,ZC,RSGXSZDWMC,CREATE_TIME".split(",").map(column=>StructField(column,StringType,true))))

   /**
     * 将DataFrame保存为Mysql表
     *
     * @param dataFrame 需要保存的dataFrame
     * @param tableName 保存的mysql 表名
     * @param saveMode 保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
     * @param prop 数据库配置文件
     */
   def save2MySQLTable(dataFrame: DataFrame,tableName:String,saveMode: SaveMode,prop: Properties)={
      if(saveMode == SaveMode.Overwrite){//先truncate,再append
         var conn : Connection = null
         try{
            conn = DriverManager.getConnection(prop.getProperty("url"),prop)
            val stmt = conn.createStatement
            stmt.execute(s"truncate table $tableName")
            stmt.close()
            conn.close()
         }
         catch {
            case e:Exception =>
               e.printStackTrace()
         }
      }
      dataFrame.write.mode(SaveMode.Append).jdbc(prop.getProperty("url"), tableName, prop)
   }

   /**
     * 保存单位基本信息表
     */
   def save2DwJbxx = {

   }


   /**
     * 保存警官基本信息表
     */
   def save2PoliceJbxx = {
      //性别
      val xbRDDSet = mdictDataFrame.filter("tree_label='xb'")
            .toDF("XB_AI","LABEL0","XB")
      //籍贯
      val jgRDDSet = mdictDataFrame.filter("tree_label='xzdq'")
            .toDF("JG_AI","LABEL1","JG")
            //.toDF("ID1","NAME1","JG_AI","LABEL1","JG")
      val csdRDDSet = mdictDataFrame.filter("tree_label='xzdq'").toDF("CSD_AI","LABEL2","CSD")
      csdRDDSet.take(10).foreach(x=>println(x))
      val mzRDDSet = mdictDataFrame.filter("tree_label='mz'").toDF("MZ_AI","LABEL3","MZ")
      val jkzkRDDSet = mdictDataFrame.filter("tree_label='p_jkzk'").toDF("JKZK_AI","LABEL3","JKZK")
      val zzmmRDDSet = mdictDataFrame.filter("tree_label='zzmm'").toDF("ZZMM_AI","LABEL4","ZZMM")
      val ryztRDDSet = mdictDataFrame.filter("tree_label='jy_ryzt'").toDF("RYGLZT_AI","LABEL5","RYLBZT")

      val resPoliceDataFrame = policeDataFrame
            .join(xbRDDSet,Seq("XB_AI"))
                  .join(sfzDataFrame,Seq("JMSFHM"),"left")
            .join(jgRDDSet,Seq("JG_AI"),"left")
                  .join(csdRDDSet,Seq("CSD_AI"),"left")
                  .join(mzRDDSet,Seq("MZ_AI"),"left")
                  .join(jkzkRDDSet,Seq("JKZK_AI"),"left")
                  .join(zzmmRDDSet,Seq("ZZMM_AI"),"left")
                  .join(ryztRDDSet,Seq("RYGLZT_AI"),"left")
               .select("XM","JMSFHM,JH,XB,CSRQ,JG,CSD,MZ,JKZK,CJGZSJ,ZZMM,CJZZRQ,RYLBZT,ZC,RSGXSZDWMC,CREATE_TIME".split(",").toSeq:_*)
                 // .printSchema()

      save2MySQLTable(resPoliceDataFrame,"t_jy_jbxx",SaveMode.Overwrite,properties)
      //resPoliceDataFrame.write.mode(SaveMode.Overwrite).jdbc(url,"t_jy_jbxx",properties)
   }

}


object PoliceApp {
   def main(args: Array[String]): Unit = {
      Police.save2PoliceJbxx
   }
}