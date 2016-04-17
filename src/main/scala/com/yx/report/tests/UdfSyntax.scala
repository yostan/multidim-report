package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/13
 */
object UdfSyntax {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hive-udf")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function date_fmt as 'com.yx.report.udf.DateFormatUDF'")
    ssc.sql("create temporary function change_money as 'com.yx.report.udf.ChangeMoney'")

    val df1 = ssc.sql("select id,cast(change_money(money) as decimal(12,2)) from oracle23.t4")
    df1.foreach(println)
    sc.stop()
  }

}
