package com.kmjy.DBUtils

object MySqlPoolManager {
   var mysqlManager: MySqlPool = _

   def getMysqlManager: MySqlPool = {
      synchronized {
         if (mysqlManager == null) {
            mysqlManager = new MySqlPool
         }
      }
      mysqlManager
   }
}