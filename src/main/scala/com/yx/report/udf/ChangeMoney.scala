package com.yx.report.udf


import org.apache.hadoop.hive.ql.exec.UDF

import scala.util.{Failure, Success, Try}


/**
 * Created by naonao on 2016/4/13
 */
class ChangeMoney extends UDF {

  def evaluate(money: String): String = {

    var v_money = money
    if(v_money == null || v_money.trim == "" || v_money == "NULL"){
      v_money = "0.00"
    } else if(v_money.startsWith("(") && v_money.endsWith(")")){
      v_money = v_money.replace("(", "")
      v_money = v_money.replace(")", "")
      v_money = "-" + v_money
    }else{
      v_money = Try(v_money.toInt) match {
        case Success(n) => n.toString
        case Failure(_) => "0.01"
      }
    }
    v_money = v_money.replace(",", "")
    v_money
  }

}
