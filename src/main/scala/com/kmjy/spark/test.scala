package com.kmjy.spark
import java.text.SimpleDateFormat
import java.util.Date

import com.kmjy.spark.teststring.{llstr, pattren}
import com.kmjy.util.Pinyin
import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.util.matching.Regex

object test extends App {
   println(Pinyin.ToFirstChar("叶栋").toUpperCase) //转为首字母大写
   val msg = "1998.09--2001.09  昆明大学学生|2001.09--2002.09  待业|2002.09--2003.09  云南省昆明监狱入监集训监区见习|2003.09--2006.10  云南省昆明监狱入监集训监区科员|（2002.09--2005.07在云南大学夜大本科经济学专业学习）|2006.10--2009.11  云南省昆明监狱入监集训监区副主任科员|2009.11--2012.10  云南省昆明监狱入监集训监区主任科员|2012.10--2017.04  云南省昆明监狱一监区三级警长|2017.04--2019.02  云南省昆明监狱五监区三级警长|2019.02--         云南省昆明监狱四监区三级警长"
   val line = "乡科级正职"
   println(

      line.split("（")(0)

   )
   private val configFactory =  ConfigFactory.load()
   private val mySqlConfig = configFactory.getConfig("mysql")
   private val tableConfig = configFactory.getConfig("savetable")
   val fileConfig = configFactory.getConfig("file")
   val rootpath = fileConfig.getString("rootpath")
   private val sparkConf = new SparkConf().setMaster("local[*]")
         .setAppName("kmjy datatransformation")
   private val spark = SparkSession.builder().config(sparkConf).getOrCreate()
   private val sc = spark.sparkContext
   private val policeRDD = sc.textFile(rootpath+fileConfig.getString("test"))
   //policeRDD.collect().foreach(x=>println(x))

   private  val headerPolice = policeRDD.first()
   private val policeFullRDD = policeRDD.filter(_!=headerPolice).map(_.split(",",-1))
   private val policeRDDRows = policeFullRDD
         .map(line=>Row(
            line(0),line(1),line(2),
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

   val llRDDRows =
   policeRDD.filter(_.contains("（")).map(_.split(",",-1)).map(
      line=>Row(line(0),line(1),line(2))
   )
         //.collect().take(10).foreach(x=>println(x))

   val llDataframe = spark.createDataFrame(llRDDRows,
      StructType("XM,JMSFHM,JL".split(",").map(column=>StructField(column,StringType,true))))
   val llDetail = llDataframe.explode("JL","LL_DETAIL"){
      llinfo:String => llinfo.split("\\|")
   }.select("XM","JMSFHM","LL_DETAIL")

   val llRDDDetail = llDetail.rdd.map(x=>Row(x(0),x(1),
      {
         val llstr = x(2).toString
         if(llstr == ""){
            null
         }
         else{
         val str = llstr.split("  ")
         str.length match {
            case  1=> {
               val pattren = new Regex("\\d{4}.\\d{2}-.*\\d{4}.\\d{2}")
               val result = pattren.findFirstIn(llstr)
               result match {
                  case Some(x) => {
                     val arr = x.split("-")
                     arr(0)
                  }
                  case _ => null
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
            case 5 =>{
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
               " "
            }
         }
         }else{
            null
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
            null
         }
      }
   ))
   val policeLLDataFrame = spark.createDataFrame(llRDDDetail,
      StructType("XM,JMSFHM,QSRQ,JZRQ,SFHZW".split(",").map(column=>StructField(column,StringType,true))))
   policeLLDataFrame.collect().foreach(x=>println(x))
   //llRDDDetail.collect().foreach(x=>println(x))
}
