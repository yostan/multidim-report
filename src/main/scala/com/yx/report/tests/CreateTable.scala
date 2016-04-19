package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/18
 */
object CreateTable {
  def main(args: Array[String]) {
    if(args.length < 1){
      System.err.println("Usage: CreateTable <table_id_condition_large>")
      System.err.println("example: CreateTable 64")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Create-Table")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    val df1 = ssc.sql(s"select table_id, table_sql from mysql59.tb_log_model_register where table_id > ${args(0)}")
    val df2 = df1.map(c => c.getAs[String]("table_sql"))
    val df3 = df2.map(c => c.toUpperCase.replace("NOT NULL", "").
      replace("NULL DEFAULT NULL", "").
      replace("ENGINE = MYISAM", "").
      replace("DEFAULT CHARSET = UTF8", "").
      replace("COLLATE = UTF8_UNICODE_CI", "").
      replace("COMMENT=", "COMMENT ")
      ).map{s =>
      if(!s.contains("IF NOT EXISTS")){
        s.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS ")
      } else {
        s
      }
    }

    println("table count: " + df3.count())
    df3.collect().foreach(ssc.sql)   //df3.foreach(ssc.sql): nest rdd is not support,will throw nullPointer exception

    sc.stop()
  }

}
