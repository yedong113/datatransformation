package com.kmjy.util

import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.{HanyuPinyinCaseType, HanyuPinyinOutputFormat, HanyuPinyinToneType}

/**
  *
  */
object Pinyin {
   /**
     *
     * @param chinese
     * @return
     */
   def ToFirstChar(chinese:String):String={
      var pinyinStr = ""
      val newChar = chinese.toCharArray //转为单个字符
      val defaultFormat = new HanyuPinyinOutputFormat()
      defaultFormat.setCaseType(HanyuPinyinCaseType.LOWERCASE)
      defaultFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE)
      for (i <- 0 until  newChar.length){
         if (newChar(i) > 128){
            pinyinStr += PinyinHelper.toHanyuPinyinStringArray(newChar(i), defaultFormat)(0).charAt(0)
         }
         else{
            pinyinStr += newChar(i)
         }
      }
      pinyinStr
   }
}
