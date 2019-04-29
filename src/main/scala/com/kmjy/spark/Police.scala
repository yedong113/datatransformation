package com.kmjy.spark
import java.sql.{Connection, DriverManager}
import java.util.{Date, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql._
import java.text.SimpleDateFormat
import java.util.logging.Logger

import com.kmjy.DBUtils.OperatorMySql
import com.kmjy.util.{MessageDigester, Pinyin}

import scala.util.matching.Regex



class Police extends Serializable {
   private val configFactory =  ConfigFactory.load()
   private val mySqlConfig = configFactory.getConfig("mysql")
   private val tableConfig = configFactory.getConfig("savetable")
   private val fileConfig = configFactory.getConfig("file")
   private val rootpath = fileConfig.getString("rootpath")
   private val properties = new Properties()
   properties.setProperty("user",mySqlConfig.getString("username"))
   properties.setProperty("password",mySqlConfig.getString("password"))
   properties.setProperty("url",mySqlConfig.getString("url"))
   private val sparkConf = new SparkConf().setMaster("local[*]")
         .setAppName("kmjy datatransformation")

   @transient
   private val spark = SparkSession.builder().config(sparkConf).config("spark.debug.maxToStringFields", "100").getOrCreate()

   @transient
   private val sc = spark.sparkContext

   private val sqlContext  = spark.newSession().sqlContext

   private val policeRDD = sc.textFile(rootpath+fileConfig.getString("A01"))
   private val sfzRDD = sc.textFile(rootpath+fileConfig.getString("sfzhjh"))
   private val mdictRDD = sc.textFile(rootpath+fileConfig.getString("mdict"))
   private val dwRDD = sc.textFile(rootpath+fileConfig.getString("B01"))
   private val zwRDD = sc.textFile(rootpath+fileConfig.getString("A02"))
   private val zwzjRDD = sc.textFile(rootpath+fileConfig.getString("A05"))
   private val zyjsRDD = sc.textFile(rootpath+fileConfig.getString("A06"))
   private val xlxwRDD = sc.textFile(rootpath+fileConfig.getString("A08"))
   private val jcRDD = sc.textFile(rootpath+fileConfig.getString("A14"))
   private val jtcyRDD = sc.textFile(rootpath+fileConfig.getString("A36"))//
   private val tcryRDD = sc.textFile(rootpath+fileConfig.getString("A30"))//
   private val hydwbmRDD = sc.textFile(rootpath+fileConfig.getString("hydwbm"))//

   def getDictDataFrame:DataFrame = {
      val dictDataFrame=OperatorMySql.getDFFromeMysql(sqlContext,fileConfig.getString("mdicttable"))
      dictDataFrame.printSchema()
      dictDataFrame.filter("tree_label='xldm'").collect().foreach(x=>println(x))
      dictDataFrame.filter("tree_label='xwdm'").collect().foreach(x=>println(x))
      dictDataFrame
   }





   private val hydwbmHeader = hydwbmRDD.first()
   private val hydwbmRDDRows = hydwbmRDD.filter(_!=hydwbmHeader).map(_.split(",",-1))
         .map(line=>Row(line(1),line(2),line(4)))

   private val hydwbmDataFrame = spark.createDataFrame(hydwbmRDDRows,
      StructType("JGBM,DWCODE,ORDER".split(",").map(column=>StructField(column,StringType))))

   /**
     * 退出人员表
     * 姓名	身份证号	退出方式	退出时间	退出后去向	批准单位
     *
     */
   private val tcryHeader = tcryRDD.first()
   private val tcryRDDRows = tcryRDD.filter(_!=tcryHeader).map(_.split(",",-1))
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),
            line(5),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val keys = line(1)+line(3)
               MessageDigester.getSh1(keys)
            }
         ))

   private val tcryDataFrame = spark.createDataFrame(tcryRDDRows,
      StructType("XM,JMSFHM,TCFS,TCSJ,TCHQX,PZDW,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))

   private val monthsDataFrame = OperatorMySql.getDFFromeMysql(sqlContext,"months")




   /**
     * 家庭成员及社会关系信息表							
     * 姓名	身份证号	家庭成员姓名	称谓	出生日期	政治面貌	工作单位及职务	排序
     */
   private val jtcyHeader = jtcyRDD.first()
   private val jtcyRDDRows = jtcyRDD.filter(_!=jtcyHeader).map(_.split(",",-1))
         .map(line=>Row(
            line(0),line(1),line(2),line(2),line(3),line(4),
            line(5),line(6),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val keys = line(1)+line(2)+line(3)
               MessageDigester.getSh1(keys)
            }
         ))
   private val jtcyDataFrame = spark.createDataFrame(jtcyRDDRows,
      StructType("XM,JMSFHM,RYXM,RYYGRGXMC,RYYGRGXMC_AI,RYCSRQ,ZZMM_AI,RYGZDWMC,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))


   /**
     * 奖惩信息表
      姓名	身份证号	奖惩名称	奖惩名称代码	批准日期	批准机关	批准机关级别	受奖惩时职务层次	撤销日期	批准机关性质
     */
   private val jcHeader = jcRDD.first()
   private val jcRDDRows = jcRDD.filter(_!=jcHeader).map(_.split(",",-1))
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),
            line(5),line(6),line(8),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val keys = line(1)+line(4)
               MessageDigester.getSh1(keys)
            }
         ))

   private val jcDataFrame = spark.createDataFrame(jcRDDRows,
      StructType("XM,JMSFHM,JCMC,JCLB,PZRQ,JLPZJGMC,PZJCJGJB,CXRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))

   private val xlxwHeader = xlxwRDD.first()
   private val xlxwRDDRows = xlxwRDD.filter(_!=xlxwHeader).map(_.split(",",-1))
         .map(line=>Row(
            line(0),line(1),line(2),line(6),line(7),line(13),line(9),
            line(10),line(11),line(4),
            line(5),line(8),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val keys = line(1)+line(3)+line(6)+line(9)
               MessageDigester.getSh1(keys)
            }
         ))
   private val xlxwDataFrame = spark.createDataFrame(xlxwRDDRows,
      StructType("XM,JMSFHM,ZGXL,RXRQ,BYYRQ,XZ,XXDWMC,SXZYMC,SXZYLB,XWXX,XW,XWSYRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))




   private val zyjsHeader = zyjsRDD.first()
   private val zyjsRDDRows = zyjsRDD.filter(_!=zyjsHeader).map(_.split(",",-1))
         .map(line=>Row(
            line(0),line(1),line(2),line(3),line(4),
            line(5),line(6),
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val keys=line(1)+line(2)
               MessageDigester.getSh1(keys)
            }
         ))

   private val zyjsDataFrame = spark.createDataFrame(zyjsRDDRows,
      StructType("XM,JMSFHM,ZYJSZGZC,JSZL,HDZGRQ,HDZGTJ,PWHHKSMC,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))




   private  val headerPolice = policeRDD.first()
   private val policeFullRDD = policeRDD.filter(_!=headerPolice).map(_.split(",",-1))
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
               val zpAddress = "/res/j_header/"+line(1)+".jpg"
               zpAddress
            },
            {
               val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
               dateFormat.format( new Date() )
            },
            {
               val encodeString = line(0)+line(1)
               MessageDigester.getSh1(encodeString)
            }
         ))

   private val dwHeader = dwRDD.first()
   private val dwRDDRows = dwRDD.filter(_!=dwHeader).map(_.split(",",-1)).map(line=>Row(
      line(0),line(1),line(2),
      {

         if(line(3)=="云南昆明盘龙"){
            "530103"
         }
         else{
            ""
         }
      },
      line(4),
      line(5),line(6),line(8),line(9),
      line(15),line(16),line(17),line(18),
      {
         val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
         dateFormat.format( new Date() )
      },
      {
         val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss") //可以方便地修改日期格式
         dateFormat.format( new Date() )
      },
      {
         val keys = line(2)
         //println(MessageDigester.getSh1(keys))
         MessageDigester.getSh1(keys)
      }
   ))
   private val dwDataFrame = spark.createDataFrame(dwRDDRows,
      StructType("JGMC,JGJC,JGBM,SZZQ,LSGX,JGJB,JGLB,XZBZS,SYBZS,NSJGLDZS,ZZLDZS,FZLDZS,BZ,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))

   private val a02header = zwRDD.first()
   private val zwRDDRows = zwRDD.filter(_!=a02header).map(_.split(","))
         .map(line=>Row(
            line(0),line(1),line(2),
            {
               if(line(3)=="是"){
                  "1"
               }
               else{
                  "2"
               }
            },
            line(4),line(5),
            {
               if(line(6)=="是"){
                  "1"
               }
               else{
                  "2"
               }
            },
            line(7),line(8),line(9),
            line(10),line(11),
            {
               if(line(12)=="是"){
                  "1"
               }
               else{
                  "2"
               }
            },
            line(13),
            line(14),line(15),line(16),line(17),line(18),
            {
               val keys = line(1)+line(2)+line(9)+line(14)+line(5)
               MessageDigester.getSh1(keys)
            }
         ))
   private val zwDataFrame = spark.createDataFrame(zwRDDRows,
      StructType("XM,JMSFHM,JGMC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,RID".split(",").map(column=>StructField(column,StringType))))


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
            },
            {
               val keys = line(1)+line(3)+line(4)
               MessageDigester.getSh1(keys)
            }
         ))
   private val zwzjDataFrame = spark.createDataFrame(zwzjRDDRows,
      StructType("XM,JMSFHM,ZWCC_AI,PZRQ,PZWH,ZZRQ,ZT,CREATE_TIME,UPDATE_TIME,RID".split(",").map(column=>StructField(column,StringType))))



   //数据字典
   private val header = mdictRDD.first()
   private val mdictRDDRows = mdictRDD.filter(_!=header)
         .map(line=>line.split(";")).map(line=>Row(line(5),line(6),line(7)))
   //private val mdictRDDRows1 = dictRDD.map(line=>Row(line))

   private val mdictDataFrame:DataFrame = spark.createDataFrame(mdictRDDRows,
      StructType("tree_alias,tree_label,tree_value".split(",").map(column=>StructField(column,StringType))))



   //身份证信息
   private val header2 = sfzRDD.first()
   private val sfzRDDRows = sfzRDD.filter(_!=header2).map(_.split(","))
         .map(line=>Row(line(0),line(1))).cache()
   private val sfzDataFrame:DataFrame = spark.createDataFrame(sfzRDDRows,
      StructType("JMSFHM,JH".split(",").map(column=>StructField(column,StringType))))


   private val policeDataFrame = spark.createDataFrame(policeRDDRows,
      StructType("XM,JMSFHM,XB_AI,CSRQ,JG_AI,CSD_AI,MZ_AI,JKZK_AI,CJGZSJ,ZZMM_AI,CJZZRQ,RYGLZT_AI,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME,SHA1".split(",").map(column=>StructField(column,StringType))))

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

   def save2PoliceTcry:Boolean = {
      val resTcryRDDRows = tcryDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,TCFS,TCSJ,TCHQX,PZDW,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,TCFS,TCSJ,TCHQX,PZDW,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_tcryxx"),resTcryRDDRows,"XM,SFZH,JH,TCFS,TCSJ,TCHQX,PZDW,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resTcryRDDRows,tableConfig.getString("t_jy_tcryxx"),SaveMode.Overwrite,properties)
   }

   /**
     * 保存警员家庭成员及社会关系
     */
   def save2PoliceJtcy:Boolean ={
      val zzmmRDDSet = mdictDataFrame.filter("tree_label='zzmm'").toDF("ZZMM_AI","LABEL4","ZZMM")
      val jtgxRDDSet = mdictDataFrame.filter("tree_label='p_jtgx'").toDF("RYYGRGXMC_AI","LABEL4","RYYGRGXDM")
      val resJtcyDataFrame = jtcyDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left")
            .join(zzmmRDDSet,Seq("ZZMM_AI"),"left")
            .join(jtgxRDDSet,Seq("RYYGRGXMC_AI"),"left")
            .select("XM","JMSFHM,JH,RYXM,RYYGRGXMC_AI,RYYGRGXDM,RYCSRQ,ZZMM,RYGZDWMC,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,RYXM,RYYGRGXMC,RYYGRGXDM,RYCSRQ,RYZZMM,RYGZDWMC,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_jtcyjshgx"),resJtcyDataFrame,"XM,SFZH,JH,RYXM,RYYGRGXMC,RYYGRGXDM,RYCSRQ,RYZZMM,RYGZDWMC,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resJtcyDataFrame,tableConfig.getString("t_jy_jtcyjshgx"),SaveMode.Overwrite,properties)
   }


   /**
     * 保存
     */
   def save2PoliceJc:Boolean = {
      val resJcDataframe = jcDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,JCMC,JCLB,PZRQ,JLPZJGMC,PZJCJGJB,CXRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,JCMC,JCLB,PZRQ,JLPZJGMC,PZJCJGJB,CXRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_jcxx"),resJcDataframe,"XM,SFZH,JH,JCMC,JCLB,PZRQ,JLPZJGMC,PZJCJGJB,CXRQ,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resJcDataframe,tableConfig.getString("t_jy_jcxx"),SaveMode.Overwrite,properties)
   }


   /**
     * 保存单位基本信息表DBBM,DWMC,DWJC,DWMCLB,DWLSGX,DWJB,DWSZZQ
     */
   def save2DwJbxx:Boolean  = {
      val resDwDataFrame =dwDataFrame.join(hydwbmDataFrame,Seq("JGBM"),"left")
                  .select("JGMC","JGJC,JGBM,SZZQ,DWCODE,ORDER,CREATE_TIME,UPDATE_TIME,RID".replace(" ","").split(",").toSeq:_*)
                  .toDF("DWMC,DWJC,DWBM,DWSZZQ,DWCODE,ORDER,CREATE_TIME,UPDATE_TIME,RID".replace(" ","").split(",").toSeq:_*)
      //resDwDataFrame.printSchema()
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_dw_jbxx"),resDwDataFrame,"DWJC,DWBM,DWSZZQ,DWCODE,ORDER,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resDwDataFrame,tableConfig.getString("t_dw_jbxx"),SaveMode.Overwrite,properties)
      true
   }

   /**
     * 保存警员学历学位信息
     */
   def save2PoliceXlxw:Boolean = {
      val resXlxwDataFrame = xlxwDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,ZGXL,RXRQ,BYYRQ,XZ,XXDWMC,SXZYMC,SXZYLB,XWXX,XW,XWSYRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,ZGXL,RXRQ,BYYRQ,XZ,XXDWMC,SXZYMC,SXZYLB,XWXX,XW,XWSYRQ,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_xlxwxx"),resXlxwDataFrame,"XM,SFZH,JH,ZGXL,RXRQ,BYYRQ,XZ,XXDWMC,SXZYMC,SXZYLB,XWXX,XW,XWSYRQ,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resXlxwDataFrame,tableConfig.getString("t_jy_xlxwxx"),SaveMode.Overwrite,properties)
   }

   /**
     * 保存单位编制信息
      */
   def save2DwBz :Boolean = {
      val resDwBzDataFrame = dwDataFrame.select("JGBM","XZBZS,SYBZS,NSJGLDZS,ZZLDZS,FZLDZS,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("DBBM,XZBZS,SYBZS,JGLDZS,ZZLDZS,FZLDZS,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_dw_bzxx"),resDwBzDataFrame,"DBBM,XZBZS,SYBZS,JGLDZS,ZZLDZS,FZLDZS,CREATE_TIME,UPDATE_TIME".split(","))
//      save2MySQLTable(resDwBzDataFrame,tableConfig.getString("t_dw_bzxx"),SaveMode.Overwrite,properties)
   }

   /**
     * 保存警员职务信息
     * 需要联合警员身份证信息(取警号)和组织机构信息(取机构编号)
   */
   def save2PoliceZW:Boolean = {
      val dwDataFrameTmp = dwDataFrame.drop("RID")
      val reszwDataFrame = zwDataFrame.join(sfzDataFrame,Seq("JMSFHM"),"left").join(dwDataFrameTmp,Seq("JGMC"),"left")
            .select("XM","JH,JMSFHM,JGBM,JGJC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,JH,SFZH,JGBM,RZJGMC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)

//      reszwDataFrame.collect().take(10).foreach(x=>println(x))
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_zwxx"),reszwDataFrame,"XM,JH,SFZH,JGBM,RZJGMC,SFLDCY,CYLB,ZWMC,SFLDZW,ZWPX,JTNPX,RZSJ,RZWH,XBRYFS,SFPGTB,RZZT,MZSJ,MZWH,ZWBDYYZS,ZWSCBZ,ZZW,CREATE_TIME,UPDATE_TIME".split(","))
//      save2MySQLTable(reszwDataFrame,tableConfig.getString("t_jy_zwxx"),SaveMode.Overwrite,properties)
   }

   /**
     * 统计单位警员人员调度信息
     */
   def statDWPoliceDD = {
      val dwDataFrameTmp = dwDataFrame.drop("RID")
      val policeDataFrameTmp = policeDataFrame.drop("RID")
      val zwddDataFrame = zwDataFrame.join(dwDataFrameTmp,Seq("JGMC"),"left")
            .select("XM","JMSFHM,JGBM,JGJC,RZSJ,RZZT,MZSJ".split(",").toSeq:_*)
            .crossJoin(monthsDataFrame)
            .select("XM","JMSFHM,JGBM,JGJC,RZSJ,RZZT,MZSJ,MONS".split(",").toSeq:_*)

      //每月人员调动 -- 任职
      val monthsPolceRz = zwddDataFrame.filter(x=>{
         val rzrq = x.getString(x.fieldIndex("RZSJ")).substring(0,6)
         val statMonts = x.getString(x.fieldIndex("MONS"))
         if (rzrq==statMonts) true else false
      }).cache()

      //每月人员调动 -- 免职
      val monthsPolceMz = zwddDataFrame.na.drop(Array("MZSJ")).filter(x=>{
         val mzsj = x.getString(x.fieldIndex("MZSJ"))
         if(mzsj=="") {
            false
         }
         else{
            val statMonts = x.getString(x.fieldIndex("MONS"))
            if (mzsj.substring(0,6)==statMonts) true else false
         }
      })


      val monthsPolicezwxx = zwddDataFrame.filter(x=>{
         var rzrq = x.getString(x.fieldIndex("RZSJ"))
         if (rzrq.length()<6) rzrq = rzrq+"01"
         var mzsj = x.getString(x.fieldIndex("MZSJ"))
         if (mzsj.length()<6) mzsj = mzsj+"01"
         val rzzt = x.getString(x.fieldIndex("RZZT"))
         val statMonts = x.getString(x.fieldIndex("MONS"))+"31"
         var ret = false
         var ret1 = false
         if(rzzt=="在任"){
            if(rzrq<statMonts) ret=true
         }
         if(rzzt=="已免"){
            if(rzrq<statMonts && mzsj>statMonts) ret1=true
         }
         if(ret || ret1) true else false
      }).cache()
      val result0 = monthsPolicezwxx.groupBy("MONS","JGJC","JGBM").count().withColumnRenamed("count","JYRS").toDF("MONS","JGJC","JGBM","JYRS")
      val result1 = monthsPolceRz.groupBy("MONS","JGBM").count().withColumnRenamed("count","DRRS").toDF("MONS","JGBM","DRRS")
      val result2 = monthsPolceMz.groupBy("MONS","JGBM").count().withColumnRenamed("count","DCRS").toDF("MONS","JGBM","DCRS")
      val result3 = result0.join(result1,Seq("MONS","JGBM"),"left").join(result2,Seq("MONS","JGBM"),"left")
                  .select("MONS","JGBM","JGJC","JYRS","DRRS","DCRS")
                  .toDF("MONS","JGBM","JGMC","JYRS","DRRS","DCRS")
      val code = (mons:String,bmbm:String) => {
         val keys = mons+bmbm
         MessageDigester.getSh1(keys)
      }
      import org.apache.spark.sql.functions.udf
      val udf_rid =udf(code)
      val result = result3.na.drop(Array("JGBM")).na.fill(0).toDF().withColumn("RID",udf_rid(result3("MONS"),result3("JGBM"))).cache()
      OperatorMySql.insertOrUpdateDFtoDBUserPool("t_jy_jlddfx",result,"JYRS,DRRS,DCRS".split(","))
      //.collect().take(100).foreach(x=>println(x))
   }

   /**
     * 保存警员专业信息
     */
   def save2PoliceZyjs:Boolean = {
      val resZyjsDataFrame = zyjsDataFrame
            .join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,ZYJSZGZC,JSZL,HDZGRQ,HDZGTJ,PWHHKSMC,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,ZYJSZGZC,JSZL,HDZGRQ,HDZGTJ,PWHHKSMC,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
      //save2PoliceZwzj
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_zyjszgzcxx"),resZyjsDataFrame,"XM,SFZH,JH,ZYJSZGZC,JSZL,HDZGRQ,HDZGTJ,PWHHKSMC,CREATE_TIME,UPDATE_TIME".split(","))
//      save2MySQLTable(resZyjsDataFrame,tableConfig.getString("t_jy_zyjszgzcxx"),SaveMode.Overwrite,properties)
   }


   /**
     *保存警员职务层次
     */
   def save2PoliceZwzj:Boolean = {
      //职务层次
      val zwccRDDSet = mdictDataFrame.filter("tree_label='jy_zwjb'")
            .toDF("ZWCC_AI","LABEL1","ZWCC")
      val resZwzjDataFrame = zwzjDataFrame
            .join(zwccRDDSet,Seq("ZWCC_AI"),"left")
            .join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,ZWCC_AI,ZWCC,PZRQ,PZWH,ZZRQ,ZT,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,ZJMC,ZJ,ZJPZRQ,ZJPZWH,ZJZZRQ,ZJZT,CREATE_TIME,UPDATE_TIME,RID".split(",").toSeq:_*)
//      resZwzjDataFrame.collect().take(10).foreach(x=>println(x))
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_zjxx"),resZwzjDataFrame,"XM,SFZH,JH,ZJMC,ZJ,ZJPZRQ,ZJPZWH,ZJZZRQ,ZJZT,CREATE_TIME,UPDATE_TIME".split(","))
      //save2MySQLTable(resZwzjDataFrame,tableConfig.getString("t_jy_zjxx"),SaveMode.Overwrite,properties)
   }

   private val llRDDRows =
      policeRDD.filter(_.contains("（")).map(_.split(",",-1)).map(
         line=>Row(line(0),line(1),line(28))
      )
   private val llDataframe = spark.createDataFrame(llRDDRows,
      StructType("XM,JMSFHM,JL".split(",").map(column=>StructField(column,StringType))))
   private val llDetail = llDataframe.explode("JL","LL_DETAIL"){
      llinfo:String => llinfo.split("\\|")
   }.select("XM","JMSFHM","LL_DETAIL")

   def save2PoliceLl:Boolean = {
      val llRDDDetail = llDetail.rdd.map(x=>Row(x(0),x(1),
         {
            val llstr = x(2).toString
            if(llstr == ""){
               ""
            }
            else{
               val str = llstr.split("  ")
               str.length match {
                  case  1=> {
                     val pattren = new Regex("\\d{4}.\\d{2}-.*\\d{4}.\\d{2}")
                     val result = pattren.findFirstIn(llstr)
                     result match {
                        case Some(x) =>
                           val arr = x.split("-")
                           arr(0)
                        case _ => ""
                     }
                  }
                  case 2=>{
                     val timestramtp = str(0)
                     val timearr = timestramtp.split("--")
                     if(timearr.length==2){
                        timearr(0)
                     }
                  }
                  case 3 =>{
                     val timestramtp = str(0)
                     val timearr = timestramtp.split("--")
                     if(timearr.length==2){
                        timearr(0)
                     }
                  }
                  case 5 => {
                     val tms1 = str(0)
                     val sp2 = tms1.split("--")
                     sp2(0)
                  }
               }
            }
         },
         {
            val llstr = x(2).toString
            if(llstr!=""){

               val str = llstr.split("  ")
               str.length match {
                  case  1=> {
                     val pattren = new Regex("\\d{4}.\\d{2}-.*\\d{4}.\\d{2}")
                     val result = pattren.findFirstIn(llstr)
                     result match {
                        case Some(x) => {
                           val arr = x.split("-")
                           arr(arr.length-1)
                        }
                        case _ => null
                     }
                  }
                  case 2=>{
                     val timestramtp = str(0)
                     val timearr = timestramtp.split("--")
                     if(timearr.length==2){
                        timearr(1)
                     }
                  }
                  case 3 =>{
                     val timestramtp = str(0)
                     val timearr = timestramtp.split("--")
                     if(timearr.length==2){
                        timearr(1)
                     }
                  }
                  case 5 =>{
                     "至今"
                  }
               }
            }else{
               ""
            }

         },
         {
            val llstr = x(2).toString.replace("）","").replace("（","")
            if (llstr!=""){

               val str = llstr.split("  ")
               str.length match {
                  case  1=> {
                     var p_t=new Regex("[\u4e00-\u9fa5].*")
                     p_t.findFirstIn(llstr) match {
                        case Some(x) => x
                        case _ =>
                     }
                  }
                  case 2=>{
                     str(1)
                  }
                  case 3 =>{
                     " "
                  }
                  case 5 =>{
                     str(4)
                  }
               }
            }
            else{
               ""
            }
         },
         {
            val s = x(1)+x(2).toString
            MessageDigester.getSh1(s)
         }
      ))
      val policeLLDataFrame = spark.createDataFrame(llRDDDetail,
         StructType("XM,JMSFHM,QSRQ,JZRQ,SFHZW,RID".split(",").map(column=>StructField(column,StringType))))
      val result =  policeLLDataFrame.na.drop().join(sfzDataFrame,Seq("JMSFHM"),"left")
            .select("XM","JMSFHM,JH,QSRQ,JZRQ,SFHZW,RID".split(",").toSeq:_*)
            .toDF("XM,SFZH,JH,QSRQ,JZRQ,SFHZW,RID".split(",").toSeq:_*)
      //save2MySQLTable(result,tableConfig.getString("t_jy_llxx"),SaveMode.Overwrite,properties)
      OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_llxx"),result,"XM,SFZH,JH,QSRQ,JZRQ,SFHZW".split(","))
   }

   /**
     * 保存警官基本信息表
     */
    def  save2PoliceJbxx:Boolean = {
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
            .select("XM","JMSFHM,JH,XB,CSRQ,JG,CSD,MZ,JKZK,CJGZSJ,ZZMM,CJZZRQ,RYLBZT,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME,SHA1".split(",").toSeq:_*)
             .toDF("XM,SFZH,JH,XB,CSRQ,JG,CSD,MZ,JKZK,CJGZSJ,ZZMM,CJZZRQ,RYLBZT,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME,SHA1".split(",").toSeq:_*)

       //resPoliceDataFrame.collect().foreach(x=>println(x))

       OperatorMySql.insertOrUpdateDFtoDBUserPool(tableConfig.getString("t_jy_jbxx"),resPoliceDataFrame,
          "XM,SFZH,JH,XB,CSRQ,JG,CSD,MZ,JKZK,CJGZSJ,ZZMM,CJZZRQ,RYLBZT,ZC,RSGXSZDWMC,XM_PY,RYZP,CREATE_TIME".split(","))
    }
}


object PoliceApp {
   def main(args: Array[String]): Unit = {

      /*class DetectThread() extends Thread{
         override def run(): Unit = {
            while (true){
               Thread.sleep(5000)
               println("Detect sfb police")
            }
         }
      }

      val t1 = new DetectThread
      t1.start()*/


      val police = new Police
      police.statDWPoliceDD
//      police.save2DwJbxx
//      police.save2DwBz
//      police.save2PoliceJbxx
//      police.save2DwJbxx
//      police.save2DwBz
//      police.save2PoliceJbxx
//      police.save2PoliceZW
//      police.save2PoliceJtcy
//      police.save2PoliceLl
//      police.save2PoliceXlxw
//      police.save2PoliceZwzj
//      police.save2PoliceZyjs
//      police.save2PoliceJc
//      police.save2PoliceTcry
   }
}