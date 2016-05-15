package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.{LoggerFactory, Logger}

/**
 * Created by naonao on 2016/4/25
 */
object SaveToHive {

  private val log: Logger = LoggerFactory.getLogger(SaveToHive.getClass)

  def main(args: Array[String]) {
    if(args.length < 1){
      log.error(
      """Usage: SaveToHive <arg1>
        |arg1: tableId
      """.stripMargin
      )
      System.exit(-1)
    }

    val conf = new SparkConf().setAppName("Transaction-Ops")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function fun_type_money_change as 'com.yx.report.udf.ChangeMoney'")
    ssc.sql("create temporary function fun_sum_month as 'com.yx.report.udf.FunSumMonth'")

    val df1 = ssc.sql(s"select id, batch_id, exe_sql from mysql59.tb_log_sql where id = ${args(0)}")
    val df2 = df1.sort("id", "batch_id").map(s => ((s.getAs[Int]("id"), s.getAs[Int]("batch_id")), s.getAs[String]("exe_sql")))




  }

}
