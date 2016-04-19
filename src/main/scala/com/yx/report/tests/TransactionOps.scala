package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/19
 */
object TransactionOps {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: InsertTable <table_id>")
      System.err.println("example: InsertTable 67")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Insert-Table")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("update eshore.tb_pre_jyfx_wbfx111 set lz_fee=888888 where sales_contract_nbr='2014B4S3GD0006'")

    sc.stop()
  }

}
