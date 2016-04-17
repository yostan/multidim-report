package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/3/28
 */
object HiveSyntax {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: HiveSyntax <tableName>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("HiveSyntax")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    hc.sql("use oracle23")
    println(" ************* start use oracle24 *************")

//    val h1 = hc.sql("select * from tb_cm_serv_char0 limit 10")
//    h1.foreach(println)
//
//    hc.sql("CREATE TEMPORARY FUNCTION to_number AS 'com.nexr.platform.hive.udf.GenericUDFToNumber'")
//
//    hc.sql("create table if not exists TB_PRE_CM_ATTR(SERV_ID DECIMAL(12), CHAR_ID DECIMAL(12), ATTR_ID DECIMAL(9)," +
//      "CHAR_CLASS VARCHAR(2), ATTR_VALUE1 VARCHAR(4000), ATTR_VALUE2 VARCHAR(2000), CREATE_DATE TIMESTAMP," +
//      "LIMIT_DATE TIMESTAMP, STATE CHAR(3), MODI_DATE TIMESTAMP)")
//
//    println(" ************* create table TB_PRE_CM_ATTR *************")
//
//    val hh = hc.sql("select to_number(sum_class_column_value,9999999999.00) from rpt_sum_class_item limit 10")
//    hh.foreach(println)


//    hc.sql("insert into table TB_PRE_CM_ATTR " +
//      "select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
//      "from TB_CM_SERV_CHAR0 a where a.char_type in (select to_number(sum_class_column_value,9999999999) " +
//      "from rpt_sum_class_item where sum_class_id=10213)")

    val startTime = System.currentTimeMillis()

    hc.sql(s"create table ${args(0)} AS " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR0 a LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) b " +
      s"ON(a.char_type = b.char_type) WHERE b.char_type is not null AND b.sum_class_id=10213")

    println(s" ************* insert into table ${args(0)} *************")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR1 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR2 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR3 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR4 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR5 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR6 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR7 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR8 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    hc.sql(s"insert into table ${args(0)} " +
      s"select a.serv_id,a.char_id,a.char_type,a.char_class,a.value1,a.value2,a.create_date,a.limit_date,a.state,a.mod_date " +
      s"from TB_CM_SERV_CHAR9 a LEFT JOIN ${args(0)} b ON(a.serv_id=b.serv_id) " +
      s"LEFT OUTER JOIN (select sum_class_id, cast(sum_class_column_value AS DECIMAL(10)) AS char_type from rpt_sum_class_item) c " +
      s"ON(a.char_type=c.char_type) WHERE c.char_type is not null AND c.sum_class_id=10213 AND b.serv_id is null")

    val totalTime = startTime - System.currentTimeMillis()

    println(s"************* total Time: $totalTime *************")

    sc.stop()
  }

}
