package com.yx.report.udf

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.hive.ql.exec.UDF
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/12
 */
class DateFormatUDF extends UDF {

  def evaluate(t: String, fmt: String): String = {
    if((t == null) || (fmt == null)) {
      ""
    } else {
      try{
        val s = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
        val dd = s.parse(t)
        val f = new SimpleDateFormat(fmt)
        f.format(dd)
      }catch {
        case _: Exception => t.toString
      }
    }
  }

}

object DateFormatUDF {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("hive-udf")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function date_fmt as 'com.yx.report.udf.DateFormatUDF'")

    val format = "yyyy/MMM/dd hh:mm a"
    val syn = s"""select id, date_fmt(cast(time as string),"yyyy/MMM/dd hh:mm a") from oracle23.t1"""
    println(s"[[ $syn ]]")
    val res = ssc.sql(syn)
    res.foreach(println)

    println("***************** THE END *****************")
    sc.stop()
  }
}
