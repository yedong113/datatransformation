package com.kmjy.spark

import java.sql.{Connection, DriverManager}
import java.util.{Date, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import java.text.SimpleDateFormat

import com.kmjy.util.Pinyin

object Police {
   private val configFactory =  ConfigFactory.load()
   private val mySqlConfig = configFactory.getConfig("mysql")
   private val tableConfig = configFactory.getConfig("savetable")
   val fileConfig = configFactory.getConfig("file")
   val rootpath = fileConfig.getString("rootpath")
   val url = mySqlConfig.getString("url")
   val properties = new Properties()
   properties.setProperty("user",mySqlConfig.getString("username"))
   properties.setProperty("password",mySqlConfig.getString("password"))
   properties.setProperty("url",mySqlConfig.getString("url"))

   private val sparkConf = new SparkConf().setMaster("local[*]")
         .setAppName("kmjy datatransformation")

   private val spark = SparkSession.builder().config(sparkConf).getOrCreate()

   private val sc = spark.sparkContext

   private val policeRDD = sc.textFile(rootpath+fileConfig.getString("A01"))
   private val sfzRDD = sc.textFile(rootpath+fileConfig.getString("sfzhjh"))
   private val mdictRDD = sc.textFile(rootpath+fileConfig.getString("mdict"))
   private val dwRDD = sc.textFile(rootpath+fileConfig.getString("B01"))
   private val zwRDD = sc.textFile(rootpath+fileConfig.getString("A02"))
   private val zwzjRDD = sc.textFile(rootpath+fileConfig.getString("A05"))
   private val zyjsRDD = sc.textFile(rootpath+fileConfig.getString("A06"))

   private  val headerPolice = policeRDD.first()
   private val policeFullRDD = policeRDD.filter(_!=headerPolice).map(_.split(","))
   private val policeRDDRows = policeFullRDD
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),
            line(5),line(7),line(8),line(9),line(11),
            line(12),line(16),line(18),line(20),
            {
               val xm_py = Pinyin.ToFirstChar(line(0)).toUpperCase
               xm_py
            },
            {
               val zpAddress = "/res/"+line(1)+".jpg"
               zpAddress
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            }
         ))

   private val policeJianLi = policeFullRDD.map(line=>Row(line(0),line(1),line(28)))




   private val dwHeader = dwRDD.first()
   private val dwRDDRows = dwRDD.filter(_!=dwHeader).map(_.split(",")).map(line=>Row(
      line(0),line(1),line(2),line(3),line(4),
      line(5),line(6),line(8),line(9),
      line(15),line(16),line(17),line(18),
      {
         val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
         dateFormat.format( new Date() )
      },
      {
         val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
         dateFormat.format( new Date() )
      }
   ))
   private val dwDataFrame = spark.createDataFrame(dwRDDRows,
      StructType("JGMC,JGJC,JGBM,SZZQ,LSGX,JGJB,JGLB,XZBZS,SYBZS,NSJGLDZS,ZZLDZS,FZLDZS,BZ,CREATE_TIME,UPDATE_TIME".split(",").map(column=>StructField(column,StringType,true))))

   private val a02header = zwRDD.first()
   private val zwRDDRows = zwRDD.filter(_!=header).map(_.split(","))
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),line(5),
            line(6),line(7),line(8),line(9),
            line(10),line(11),line(12),line(13),
            line(14),line(15),line(16),line(17),line(18)
         ))
   private val zwDataFrame = spark.createDataFrame(zwRDDRows,
      StructType("XM,JMSFHM,JGMC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW".split(",").map(column=>StructField(column,StringType,true))))


   private val zwzjHeader = zwzjRDD.first()
   private val zwzjRDDRows = zwzjRDD.filter(_!=zwzjHeader).map(_.split(","))
         .map(line => Row(
            line(0),line(1),
            line(3),
            line(4),line(5),line(6),line(7),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            }
         ))
   private val zwzjDataFrame = spark.createDataFrame(zwzjRDDRows,
      StructType("XM,JMSFHM,ZWCC_AI,PZRQ,PZWH,ZZRQ,ZT,CREATE_TIME,UPDATE_TIME".split(",").map(column=>StructField(column,StringType,true))))



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
      StructType("XM,JMSFHM,XB_AI,CSRQ,JG_AI,CSD_AI,MZ_AI,JKZK_AI,CJGZSJ,ZZMM_AI,CJZZRQ,RYGLZT_AI,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME".split(",").map(column=>StructField(column,StringType,true))))

   /**
     * 将DataFrame保存为Mysql表
     *
     * @param dataFrame 需要保存的dataFrame
     * @param tableName 保存的mysql 表名
     * @param saveMode 保存的模式 ：Append、Overwrite、ErrorIfExists、Ignore
     * @param prop 数据库配置文件
     */
   def save2MySQLTable(dataFrame: DataFrame,tableName:String,saveMode: SaveMode,prop: Properties) :Unit = {
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
     * 保存单位基本信息表DBBM,DWMC,DWJC,DWMCLB,DWLSGX,DWJB,DWSZZQ
     */
   def save2DwJbxx  = {
      val resDwDataFrame =dwDataFrame.select("JGMC","JGJC,JGBM,SZZQ,LSGX,JGJB,JGLB,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
            .toDF("DBBM,DWMC,DWJC,DWMCLB,DWLSGX,DWJB,DWSZZQ,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
      save2MySQLTable(resDwDataFrame,"t_dw_jbxx",SaveMode.Overwrite,properties)
   }

   /**
     * 保存单位编制信息
      */
   def save2DwBz :Unit = {
      val resDwBzDataFrame = dwDataFrame.select("JGBM","XZBZS,SYBZS,NSJGLDZS,ZZLDZS,FZLDZS,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
            .toDF("DBBM,XZBZS,SYBZS,JGLDZS,ZZLDZS,FZLDZS,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
      save2MySQLTable(resDwBzDataFrame,"t_dw_bzxx",SaveMode.Overwrite,properties)
   }

   /**
     * 保存警员职务信息
     * 需要联合警员身份证信息(取警号)和组织机构信息(取机构编号)
   */
   def save2PoliceZW:Unit = {
      val reszwDataFrame = zwDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left").join(dwDataFrame,Seq("JGMC"),"left")
            .select("XM","JH,JMSFHM,JGBM,JGJC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
            .toDF("XM,JH,SFZH,JGBM,RZJGMC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)

//      reszwDataFrame.collect().take(10).foreach(x=>println(x))
      save2MySQLTable(reszwDataFrame,"aaa.t_jy_zwxx",SaveMode.Overwrite,properties)
   }

   /**
     * 保存警员专业信息
     */
   def save2PoliceZy = {

   }


   def save2PoliceZwzj = {
      //职务层次
      val zwccRDDSet = mdictDataFrame.filter("tree_label='jy_zwjb'")
            .toDF("ZWCC_AI","LABEL1","ZWCC")
      val resZwzjDataFrame = zwzjDataFrame
            .join(zwccRDDSet,Seq("ZWCC_AI"),"left")
            .join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,ZWCC_AI,ZWCC,PZRQ,PZWH,ZZRQ,ZT,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
            .toDF("XM,JMSFHM,JH,ZJMC,ZJ,ZJPZRQ,ZJPZWH,ZJZZRQ,ZJZT,CREATE_TIME,UPDATE_TIME".split(",").toSeq:_*)
//      resZwzjDataFrame.collect().take(10).foreach(x=>println(x))
      save2MySQLTable(resZwzjDataFrame,tableConfig.getString("t_jy_zjxx"),SaveMode.Overwrite,properties)
   }


   /**
     * 保存警官基本信息表
     */
    def  save2PoliceJbxx = {
      //性别
      val xbRDDSet = mdictDataFrame.filter("tree_label='xb'")
            .toDF("XB_AI","LABEL0","XB")
      //籍贯
      val jgRDDSet = mdictDataFrame.filter("tree_label='xzdq'")
            .toDF("JG_AI","LABEL1","JG")
      //.toDF("ID1","NAME1","JG_AI","LABEL1","JG")
      val csdRDDSet = mdictDataFrame.filter("tree_label='xzdq'").toDF("CSD_AI","LABEL2","CSD")
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
            .select("XM","JMSFHM,JH,XB,CSRQ,JG,CSD,MZ,JKZK,CJGZSJ,ZZMM,CJZZRQ,RYLBZT,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME".split(",").toSeq:_*)

      save2MySQLTable(resPoliceDataFrame,tableConfig.getString("t_jy_jbxx"),SaveMode.Overwrite,properties)
   }

}


object PoliceApp {
   def main(args: Array[String]): Unit = {
//      Police.save2PoliceJbxx
//      Police.save2PoliceZW
//      Police.save2DwJbxx
//      Police.save2DwBz
      Police.save2PoliceZwzj
   }
}