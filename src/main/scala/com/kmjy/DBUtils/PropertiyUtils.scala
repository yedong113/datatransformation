package com.kmjy.DBUtils

import java.util.Properties

object PropertiyUtils {
   def getFileProperties(fileName: String, propertityKey: String): String = {
      val result = this.getClass.getClassLoader.getResourceAsStream(fileName)
      val prop = new Properties()
      prop.load(result)
      prop.getProperty(propertityKey)
   }
}
