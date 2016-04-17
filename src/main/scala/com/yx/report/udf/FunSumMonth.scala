package com.yx.report.udf

import org.apache.hadoop.hive.ql.exec.UDF

/**
 * Created by naonao on 2016/4/15
 */
class FunSumMonth extends UDF {

  def evaluate(month: String, sales_nbr: String, purchase_nbr: String): String = {
    var sum_month = ""
    var v_year = ""
    var v_year_r = ""
    var v_month = ""
    var v_month_r = ""
    if(sales_nbr == null || sales_nbr == "NULL" || sales_nbr == "") {
      if(purchase_nbr == null || purchase_nbr == "NULL" || purchase_nbr == "") return "0" else v_year_r = purchase_nbr
    } else {
      v_year_r = sales_nbr
    }

    if(v_year_r.startsWith("B")) v_year = "2007" else v_year = v_year_r.substring(0, 4)
    val ind = month.indexOf("月")
    if(ind > 0) v_month_r = month.substring(0, ind) else v_month_r = month
    val regex = """(^[0-9]*$)""".r
    v_month_r match {
      case regex(a) => if(a.length == 1) v_month = "0" + v_month_r else v_month = v_month_r
      case _ => return "0"
    }

    sum_month = v_year + v_month
    sum_month
  }

}

object FunSumMonth {
  def main(args: Array[String]) {
    val cls = new FunSumMonth()
    val a1 = cls.evaluate("1月", "2015B", "2015B")    //201501
    val a2 = cls.evaluate("12月", "B2014", "B2014")  //200712
    val a3 = cls.evaluate("4月", "", "2016B")         //201604
    val a4 = cls.evaluate("月", "2017B", "2017B")    //0
    val a5 = cls.evaluate("2月", "", "")              //0

    println(a1)
    println(a2)
    println(a3)
    println(a4)
    println(a5)
  }
}
