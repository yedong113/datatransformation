package com.kmjy.spark
import org.apache.spark.sql.catalyst.expressions.In

import scala.util.matching.Regex
object teststring extends  App {
   val pattren = new Regex("\\d{4}.\\d{2}-.*\\d{4}.\\d{2}")
   val llstr = "（2004.02-2006.02挂任云南省西山监狱党委委员、副监狱长）".replace("）","").replace("（","")
   val result = pattren.findFirstIn(llstr)
   result match {
      case Some(x) => {
         val arr = x.split("-")
         println(arr(0))
         println(arr(arr.length-1))
      }
      case _ =>
   }
   var p_t=new Regex("[\u4e00-\u9fa5].*")
   p_t.findFirstIn(llstr) match {
      case Some(x) => println(x)
      case _ =>
   }
}
