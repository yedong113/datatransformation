package com.kmjy.spark

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger

object DetectPoliceInfoFile {
   val logger = Logger.getRootLogger
   private val configFactory =  ConfigFactory.load()
   private val fileConfig = configFactory.getConfig("file")
   private val rootpath = fileConfig.getString("rootpath")

   private val fileList = genFileList

   /**
     * 判断文件是否存在
     * @param fileName 文件名路径 全路径
     * @return 文件存在返回true，否则返回flase
     */
   private def judeFileExists(fileName:String):Boolean ={
      val file = new File(fileName)
      file.exists()
   }

   /**
     * 判断文件列表中的文件是否都存在
     * @param fileList 文件列表
     * @return 若文件列表中的文件都存在返回true，否则返回false
     */
   private def judeFileExists(fileList:Array[String]) :Boolean = {
      var ret = true
      for (fileName <- fileList){
         if(!judeFileExists(fileName)) {
            //logger.error("文件缺失:"+fileName)
            ret = false
         }
      }
      ret
   }

   /**
     * 根据配置文件生成文件列表
     * @return 返回文件列表
     */
   private def genFileList = {
       val str = rootpath+fileConfig.getString("A01") + "," +
             rootpath+fileConfig.getString("sfzhjh") +","+
             rootpath+fileConfig.getString("B01")+","+
             rootpath+fileConfig.getString("A02")+","+
             rootpath+fileConfig.getString("A05")+","+
             rootpath+fileConfig.getString("A06")+","+
             rootpath+fileConfig.getString("A08")+","+
             rootpath+fileConfig.getString("A14")+","+
             rootpath+fileConfig.getString("A36")+","+
             rootpath+fileConfig.getString("A99")+","+
             rootpath+fileConfig.getString("A15")+","+
             rootpath+fileConfig.getString("A30")+","
      str.split(",")
   }

   /**
     * 监测文件是否存在 最终调用
     * @return
     */
   def detectFileExist :Boolean = {
      judeFileExists(fileList)
   }


   def moveFiles :Boolean = {
      val df = new SimpleDateFormat("yyyyMMddHHmm")
      val backpath = rootpath+"back/"+df.format(new Date())+"/"
      val dir = new File(backpath)
      if(!dir.exists()) dir.mkdir()
      for (oldFileName <- fileList){
         val last_index = oldFileName.lastIndexOf('/')
         val fname = oldFileName.substring(last_index+1,oldFileName.length)
         val newfilename = backpath+fname
         val oldfile = new File(oldFileName)
         val newfile = new File(newfilename)
         oldfile.renameTo(newfile)
      }
      true
   }


}


object DetectApp extends App {
   class DetectThread() extends Thread{
      override def run(): Unit = {
         while (true){
            Thread.sleep(10*1000)
            val ret = DetectPoliceInfoFile.detectFileExist
            if(ret){
               println("开始导入")
               DetectPoliceInfoFile.moveFiles
            }
         }
      }
   }

   val t1 = new DetectThread
   t1.start()
}