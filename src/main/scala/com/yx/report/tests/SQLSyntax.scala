package com.yx.report.tests

import java.io.{File, FileOutputStream}
import java.net.{URI, URL}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

import org.apache.spark.sql.functions.broadcast
import scala.sys.process._

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
    hiveContext.sql("use oracletest")




    val fileBuf = Source.fromFile(args(2))
    val sb = new StringBuilder()
    for(line <- fileBuf.getLines()){
      sb ++= line
    }
    val sqlll = sb.toString()

    println(s"&&&&&&&&&&&&&&&&&&&&&&&  $sqlll  &&&&&&&&&&&&&&&&&&&&&&&")
//
//    hiveContext.sql("cache table t0 as select distinct corp_id as fkid, corp_name from edp_dim_corp")
//    hiveContext.sql("cache table t1 as select distinct SUBST_ID as fkid, SUBST_NAME from TB_DIM_SUBST")
//    hiveContext.sql("cache table t2 as select distinct branch_id as fkid, branch_name from EDP_DIM_BRANCH")
//    hiveContext.sql("cache table t3 as select distinct SERV_GRP_TYPE as fkid, serv_grp_type_name from TB_DIM_SERV_GRP_TYPE")
//    hiveContext.sql("cache table t4 as select distinct cust2_tactic_id as fkid, cust2_tactic_name from TB_DIM_CUST_TACTIC_ALL")
//    hiveContext.sql("cache table t5 as select distinct cust3_tactic_id as fkid, cust3_tactic_name from TB_DIM_CUST_TACTIC_ALL")
//    hiveContext.sql("cache table t6 as select distinct CUST4_TACTIC_ID as fkid, CUST4_TACTIC_NAME from TB_DIM_CUST_TACTIC_ALL")
//    hiveContext.sql("cache table t7 as select distinct create_date as fkid, create_date_name from TB_DIM_CREATE_DATE_TMP")
//    hiveContext.sql("cache table t8 as select distinct city_village_id as fkid, city_village_name from tb_dim_city_village")
//    hiveContext.sql("cache table t9 as select distinct fee_id as fkid, fee_name from TB_DIM_FEE")
//    hiveContext.sql("cache table t10 as select distinct PAYMENT_ID as fkid, PAYMENT_NAME from TB_DIM_PAYMENT_TYPE")
//    hiveContext.sql("cache table t11 as select distinct KD_BQ_TYPE as fkid, KD_BQ_TYPE_NAME from TB_DIM_KD_BQ_TYPE")
//    hiveContext.sql("cache table t12 as select distinct kd_data_type_id as fkid, kd_data_type_name from TB_DIM_DATA_KD_TYPE")
//    hiveContext.sql("cache table t13 as select distinct speed_value as fkid, speed_value_name from TB_DIM_SPEED_VALUE_1")
//    hiveContext.sql("cache table t14 as select distinct speed_value as fkid, speed_value_name from TB_DIM_SPEED_VALUE_1")
//    hiveContext.sql("cache table t15 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t16 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t17 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t18 as select distinct bhgq_type as fkid, BHGQ_NAME from TB_DIM_BHGQ_TYPE")
//    hiveContext.sql("cache table t19 as select distinct DEVELOP_CHANNEL_NEW as fkid, CHANNEL_16_NAME from tb_dim_develop_channel_12")
//    hiveContext.sql("cache table t20 as select distinct DIVIDE_MARKET_6_DL as fkid, DIVIDE_MARKET_6_DL_NAME from TB_DIM_DIVIDE_MARKET_6")
//    hiveContext.sql("cache table t21 as select distinct DIVIDE_MARKET_6 as fkid, DIVIDE_MARKET_6_NAME from TB_DIM_DIVIDE_MARKET_6")
//    hiveContext.sql("cache table t22 as select distinct kd_disc_zl as fkid, kd_disc_zl_name from tb_dim_kd_disc_type")
//    hiveContext.sql("cache table t23 as select distinct KD_DISC_TYPE as fkid, KD_DISC_TYPE_NAME from TB_DIM_KD_DISC_TYPE")
//    hiveContext.sql("cache table t24 as select distinct TCFD_TYPE as fkid, TCDCXX from TB_DIM_TCFD_392")
//    hiveContext.sql("cache table t25 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t26 as select distinct BUNDLE_DISC_TYPE as fkid, BUNDLE_DISC_TYPE_NAME from TB_DIM_BUNDLE_DISC_TYPE")
//    hiveContext.sql("cache table t27 as select distinct IS_ADD_DISC as fkid, IS_ADD_DISC_DESC from TB_DIM_ADD_DISC_TYPE")
//    hiveContext.sql("cache table t28 as select distinct arpu_id as fkid, arpu_name from DIM_KD_ARPU")
//    hiveContext.sql("cache table t29 as select distinct ARPU_SR as fkid, ARPU_SR_NAME from TB_DIM_ARPU_392")
//    hiveContext.sql("cache table t30 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t31 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t32 as select distinct MIX_TYPE as fkid, MIX_TYPE_NAME from tb_dim_mix_type_new")
//    hiveContext.sql("cache table t33 as select distinct BUNDLE_TYPE as fkid, BUNDLE_TYPE_NAME from TB_DIM_BUNDLE_TYPE")
//    hiveContext.sql("cache table t34 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t35 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t36 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t37 as select distinct CZ_FLAG_ID as fkid, CZ_FLAG_NAME from TB_DIM_CZ_FLAG_NEW")
//    hiveContext.sql("cache table t38 as select distinct NET_DURATION_GRADE as fkid, NET_DURATION_GRADE_NAME from TB_DIM_NET_DURATION_GRADE")
//    hiveContext.sql("cache table t39 as select distinct shifou_id as fkid, shifou_name from TB_DIM_SHIFOU")
//    hiveContext.sql("cache table t40 as select distinct CZ_FLAG_ID as fkid, CZ_FLAG_NAME from TB_DIM_CZ_FLAG_Y")
//    hiveContext.sql("cache table t41 as select distinct DATA_PROD_TYPE as fkid, DATA_PROD_TYPE_NAME from TB_DIM_DATA_PROD_TYPE")
//
//
//    val h1 = hiveContext.sql(sqlll)
//
//    h1.write.format("com.databricks.spark.csv").option("header", "true").option("delimiter", "\t")
//      .save(args(0))

//    h1.repartition(1).write.format("com.databricks.spark.csv").option("header", "false").option("delimiter", "\t")
//      .save(args(1))

//    val cmd = Seq("hdfs", "dfs", "-getmerge", args(0), args(1))


    val cmd = Seq("hive", "--database", "oracletest", "-e", sqlll)
//    val exitCode = cmd.!
//    println(s"exit-code: $exitCode")
    val stat = (cmd #> new File(args(1))).!
    println(s"status: $stat")

//    val rootDir = "hdfs://132.122.1.201:8020"
//
//        val dest = rootDir + args(0)
//        val localDir = args(1)
//        val cf = new Configuration()
//
//        val fs = FileSystem.get(URI.create(dest), cf)
//    //    val input = fs.open(new Path(dest))
//    //    val output = new FileOutputStream(localDir)
//    //    IOUtils.copyBytes(input, output, 4096, true)
//        val statusArr = fs.listStatus(new Path(dest))
//        for(x <- statusArr){
//          fs.copyToLocalFile(x.getPath, new Path(localDir))
//        }



    //    h1.foreach(println)
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
