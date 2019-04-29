package com.kmjy.DBUtils

import java.sql.Connection

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.typesafe.config.ConfigFactory

class MySqlPool extends Serializable {
   private val cpds : ComboPooledDataSource = new ComboPooledDataSource(true)
   private val configFactory =  ConfigFactory.load()
   private val mysqlConfig = configFactory.getConfig("mysql")
   try {
      cpds.setJdbcUrl(mysqlConfig.getString("url"))
      cpds.setDriverClass(mysqlConfig.getString("driver"))
      cpds.setUser(mysqlConfig.getString("username"))
      cpds.setPassword(mysqlConfig.getString("password"))
      cpds.setMinPoolSize(mysqlConfig.getString("pool.jdbc.minPoolSize").toInt)
      cpds.setMaxPoolSize(mysqlConfig.getString("pool.jdbc.maxPoolSize").toInt)
      cpds.setAcquireIncrement(mysqlConfig.getString("pool.jdbc.acquireIncrement").toInt)
      cpds.setMaxStatements(mysqlConfig.getString("pool.jdbc.maxStatements").toInt)
   } catch {
      case e: Exception => e.printStackTrace()
   }

   def getConnection: Connection = {
      try {
         cpds.getConnection()
      } catch {
         case ex: Exception =>
            ex.printStackTrace()
            null
      }
   }

   def close() = {
      try {
         cpds.close()
      } catch {
         case ex: Exception =>
            ex.printStackTrace()
      }
   }
}
