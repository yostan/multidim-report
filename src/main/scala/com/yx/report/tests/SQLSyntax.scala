package com.yx.report.tests

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/3/22
 */
object SQLSyntax {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("hiveSQL-Syntax")//.setMaster("local[2]")
//    conf.set("spark.testing.reservedMemory","0")   //only local test
    val sc = new SparkContext(conf)

    val hiveContext = new HiveContext(sc)
    println("******************* start hive ops ****************************")
    hiveContext.sql("use oracle23")
    val h1 = hiveContext.sql("select * from rpt_sum_class_item limit 20")
    h1.foreach(println)
    println("******************* end hive ops ****************************")

//    val sqlc = new SQLContext(sc)
//    val df = sqlc.read.json(args(0))
//    println("------------------------------------------------------------")
//    df.show()
//    df.select(df("name"), df("age") + 2).show()
//    println("------------------------------------------------------------")
    sc.stop()
  }

}
