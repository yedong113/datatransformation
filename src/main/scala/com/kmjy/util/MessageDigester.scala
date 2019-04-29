package com.kmjy.util

import java.security.MessageDigest
import org.apache.commons.lang.StringUtils

object MessageDigester {
   def getSh1(encryptStr:String):String = try{
      if (StringUtils.isBlank(encryptStr)) return null
      //指定sha1算法
      val digest = MessageDigest.getInstance("SHA-1")
      digest.update(encryptStr.getBytes)
      //获取字节数组
      val messageDigest = digest.digest
      // Create Hex String
      val hexString = new StringBuffer
      // 字节数组转换为 十六进制 数
      var i = 0
      while ( {
         i < messageDigest.length
      }) {
         val shaHex = Integer.toHexString(messageDigest(i) & 0xFF)
         if (shaHex.length < 2) hexString.append(0)
         hexString.append(shaHex)

         {
            i += 1; i - 1
         }
      }
      // 转换为全大写
      hexString.toString.toUpperCase
   }catch {
      case e: Exception =>
         e.printStackTrace()
         throw new Exception(e)
   }
}


object MyTestApp extends App{
   println(MessageDigester.getSh1("abcdefg"))

}
