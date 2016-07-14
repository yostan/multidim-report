package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}
import scala.sys.process._

/**
 * Created by naonao on 2016/5/25
 */
object MainOps {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HiveSyntax3 <target_table_ids>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("main-ops")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function fun_type_money_change as 'com.yx.report.udf.ChangeMoney'")
    ssc.sql("create temporary function fun_sum_month as 'com.yx.report.udf.FunSumMonth'")

    //1. create table
    val df1 = ssc.sql(s"select table_id, table_sql from mysql59.tb_log_model_register where table_id = ${args(0)}")
    val df2 = df1.map(c => c.getAs[String]("table_sql")).collect()
    val df3 = df2.map { c =>
      val a1 = c.toUpperCase.replace("NOT NULL", "").
      replace("NULL DEFAULT NULL", "").
      replace("ENGINE = MYISAM", "").
      replace("DEFAULT CHARSET = UTF8", "").
      replace("COLLATE = UTF8_UNICODE_CI", "").
      replace("COMMENT=", "COMMENT ")
      if(!a1.contains("IF NOT EXISTS")){
        a1.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS ")
      }
      val ind1 = a1.indexOf("(")
      val ind2 = a1.indexOf(",")
      val str1 = a1.substring(ind1+1,ind2).trim
      val ind3 = str1.indexOf(" ")
      val str2 = str1.substring(0, ind3)

      val a2 = a1 + s""" CLUSTERED BY(${str2}) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES("TRANSACTIONAL"="TRUE")"""
      a2
    }
    df3.foreach(println)
    df3.foreach{ ss =>
      val cmd = Seq("hive", "-e", ss)
      val exitCode = cmd.!
      println(s"create-table-exit-code: $exitCode")
    }

    //2. insert/delete/update ops
    val ddf1 = ssc.sql(s"select id, batch_id, exe_sql from mysql59.tb_log_sql where id = ${args(0)}")
    val ddf2 = ddf1.map(s => ((s.getAs[Int]("id"), s.getAs[Int]("batch_id")), s.getAs[String]("exe_sql"))).partitionBy(new MyPartitioner(1)).sortByKey()

    val ddf3 = ddf2.collect().map{t =>
      t._2.toUpperCase match {
        case c if c.startsWith("DELETE FROM") || c.startsWith("UPDATE") =>
          c
        //"ACID " + c
        case c if c.startsWith("INSERT INTO") =>
          val index1 = t._2.indexOf('(')
          val index2 = t._2.indexOf(')')
          val s1 = t._2.substring(0, index1).toUpperCase
          val s2 = t._2.substring(index2 + 1).toUpperCase
          val s3 = s1.replace("INSERT INTO", "INSERT INTO TABLE")
          val s = s3 + " " + s2
          s
        case _ =>   //truncate
          t._2
      }
    }

    ddf3.foreach{x =>
      println(s"*********** sql **************: $x")
      val cmd = Seq("hive", "-i", "/home/spark/test/addudf.sh", "-e", x)
      val exitCode = cmd.!
      println(s"DDL-exit-code: $exitCode")
    }

    sc.stop()
  }
}
