package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/4/18
 */
object InsertTable {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: InsertTable <table_id>")
      System.err.println("example: InsertTable 67")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("Insert-Table")
    val sc = new SparkContext(conf)
    val ssc = new HiveContext(sc)

    ssc.sql("create temporary function fun_type_money_change as 'com.yx.report.udf.ChangeMoney'")
    ssc.sql("create temporary function fun_sum_month as 'com.yx.report.udf.FunSumMonth'")

    val df1 = ssc.sql(s"select id, batch_id, exe_sql from mysql59.tb_log_sql where id = ${args(0)}")
//    val df1 = ssc.sql(s"select id, batch_id, exe_sql from mysql59.tb_log_sql")
    val df2 = df1.sort("id", "batch_id").map(s => ((s.getAs[Int]("id"), s.getAs[Int]("batch_id")), s.getAs[String]("exe_sql")))
    println("default: " + df2.partitions.length)
    val df3 = df2.partitionBy(new MyPartitioner(2))
    println("after: " + df3.partitions.length)
//    df3.saveAsTextFile(args(1))
    df3.map{t =>
      println(t._2)
      if(t._2.indexOf('(') == -1 || t._2.indexOf(')') == -1){
        t._2
      }else{
        val index1 = t._2.indexOf('(')
        //      println(s"index1: $index1")
        val index2 = t._2.indexOf(')')
        //      println(s"index2: $index2")
        val s1 = t._2.substring(0, index1)
        val s2 = t._2.substring(index2 + 1)
        val s3 = s1.toUpperCase.replace("INSERT INTO", "INSERT INTO TABLE")
        val s = s3 + " " + s2
        s
      }
    }.collect().foreach(ssc.sql)

    sc.stop()
  }

}
