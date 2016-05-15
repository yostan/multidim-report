package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.sys.process._

/**
 * Created by naonao on 2016/4/19
 */

/**
 * spark-submit: use yarn-client mode.  (yarn-cluster will no effect)
 *  set hive.vectorized.execution.enabled=false;(vim hive-site.xml)
 */
object TransactionOps {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: TransactionOps <table_id")
      System.err.println("example: TransactionOps 911")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Transaction-Ops")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function fun_type_money_change as 'com.yx.report.udf.ChangeMoney'")
    ssc.sql("create temporary function fun_sum_month as 'com.yx.report.udf.FunSumMonth'")

    val df1 = ssc.sql(s"select id, batch_id, exe_sql from mysql59.tb_log_sql where id = ${args(0)}")
    val df2 = df1.map(s => ((s.getAs[Int]("id"), s.getAs[Int]("batch_id")), s.getAs[String]("exe_sql"))).partitionBy(new MyPartitioner(1)).sortByKey()
//    val df3 = df2.partitionBy(new MyPartitioner(1))

    val df4 = df2.map{t =>
      t._2.toUpperCase match {
        case c if c.startsWith("DELETE FROM") || c.startsWith("UPDATE") =>
         c
         //"ACID " + c
        case c if c.startsWith("INSERT INTO") =>
          val index1 = t._2.indexOf('(')
          val index2 = t._2.indexOf(')')
          val s1 = t._2.substring(0, index1)
          val s2 = t._2.substring(index2 + 1)
          val s3 = s1.toUpperCase.replace("INSERT INTO", "INSERT INTO TABLE")
          val s = s3 + " " + s2
          s
        case _ =>   //truncate
          t._2
      }
    }

    df4.collect().foreach{x =>
      println(s"*********** sql **************: $x")
      val cmd = Seq("hive", "-i", "/home/spark/test/addudf.sh", "-e", x)
      val exitCode = cmd.!
      println(s"exit-code: $exitCode")
    }

//    df4.collect().foreach{x =>
//      if(x.startsWith("ACID")) {
//        val raw_str = x.substring(5)
//        println(s"******* raw String ********: $raw_str")
//        val cmd = Seq("hive", "-e", raw_str)
//        val exitCode = cmd.!
//        println(s"exit-code: $exitCode")
//      } else {
//        println(s"*********** sql **************: $x")
//        ssc.sql(x)
//      }
//    }


//    val cmd = "hive -e \"update eshore.tb_pre_jyfx_wbfx111 set lz_fee=888888 where sales_contract_nbr='2014B4S3GD0006';\""
//    val cmd1 = Seq("hive", "-e", "update eshore.tb_pre_jyfx_wbfx111 set lz_fee=888888 where sales_contract_nbr='2014B4S3GD0006'")
//    val exit_code = cmd1.!
//    println(s"exit code: $exit_code")

    sc.stop()
  }

}
