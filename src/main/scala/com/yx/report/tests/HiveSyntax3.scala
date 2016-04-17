package com.yx.report.tests

import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/15
 */
object HiveSyntax3 {
  def main(args: Array[String]) {
    if(args.length < 1){
      System.err.println("Usage: HiveSyntax3 <target_table_ids>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("hive-syntax-3")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)


    ssc.sql("create temporary function date_fmt as 'com.yx.report.udf.DateFormatUDF'")
    ssc.sql("create temporary function change_money as 'com.yx.report.udf.ChangeMoney'")
    ssc.sql("create temporary function fun_sum_month as 'com.yx.report.udf.FunSumMonth'")

    for(id <- args){
      val df1 = ssc.sql(s"select table_id, table_sql from mysql59.tb_log_model_register where table_id = $id")

      val df2 = df1.map(c => (c.getAs[Int]("table_id"),c.getAs[String]("table_sql")))
      val df3 = df2.map(c => (c._1, c._2.replace("NOT NULL", "").
                               replace("NULL DEFAULT NULL", "").
                               replace("ENGINE = MyISAM", "").
                               replace("DEFAULT CHARSET = UTF8", "").
                               replace("COLLATE = UTF8_UNICODE_CI", "").
                               replace("COMMENT=", "COMMENT ")
      ))
      df3.foreach(println)

    }

    ssc.sql("desc default.tb_pre_jyfx_sr").foreach(println)


//    val df1 = ssc.sql("select id,cast(change_money(money) as decimal(12,2)) from oracle23.t4")
//    df1.foreach(println)
    sc.stop()
  }

}
