package com.yx.report.center

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/1
 */

/**
 *  spark read mysql:
 *    spark-shell --driver-class-path /usr/share/java/mysql-connector-java.jar
 *    spark-submit   --conf "spark.driver.extraClassPath=/usr/share/java/mysql-connector-java.jar" \
 *                   --conf "spark.executor.extraClassPath=/usr/share/java/mysql-connector-java.jar" \
 */
object ReadMysql {

  def main(args: Array[String]) {
    if(args.length < 4){
      System.err.println("Usage: ReadMysql <jdbcurl> <username> <passwd> <table_name>")
      System.exit(1)
    }

    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

//    val jdbcUrl = "jdbc:mysql://server01.bigdata.com:3306/hive"
    val jdbcUrl = args(0)
    val prop = new Properties()
    prop.setProperty("user", args(1))
    prop.setProperty("password", args(2))

    val tbls = ssc.read.jdbc(jdbcUrl, args(3), prop)

    val r1 = tbls.select("id", "batch_id", "exe_sql").sort("id", "batch_id")
    println("count: " + r1.count())
    r1.foreach(println)

    sc.stop()
  }

}
