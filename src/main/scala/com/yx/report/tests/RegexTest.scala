package com.yx.report.tests

/**
 * Created by naonao on 2016/5/16
 */
object RegexTest {

  def main(args: Array[String]) {

    val sss = "SELECT * FROM T1 A INNER JOIN IODATA.TCONFCDR5 B ON A.FCOMPANYACCOUNTID=B.FCOMPANYACCOUNTID  AND b.Fstarttime < '20150901' LEFT JOIN db.T4 C ON A.ID >= C.ID WHERE 1=1"

    val reg = """ (LEFT JOIN|RIGHT JOIN|INNER JOIN)[\s]+[\w\.]+[\s]+[\w]+[\s]+ON[\s]+[\w\.]+[\s]*([\W]+)[\w\.]+[\s]+(AND|OR)[\s]+[\w\.]+[\s]+([\W]+)[\s]+([^\s]+)""".r

    val res = reg.findAllIn(sss)
    for(reg(i,j,k,m,n) <- res){
      println(s"$i | $j | $k | $m | $n")
    }

    val specString = Array(">", "<", ">=", "<=")

    val ind = sss.indexOf(">=")
    if(ind != -1){
      val s1 = sss.substring(0,ind).trim
      val s2 = sss.substring(ind+2).trim
      println(s"s2: $s2")
      val s3 = s1.lastIndexOf(" ")
      val s4 = s2.indexOf(" ")
      val s5 = s1.substring(s3)
      val s6 = s2.substring(0,s4)
      println(s" ${s5}|${s6}")
    }


    val a1 = "CREATE TABLE ESHORE.TB_RPT_JF002(  SALES_CONTRACT_NBR   VARCHAR(1000) NULL DEFAULT NULL COMMENT '关联销售合同编号',PROJECT_NAME VARCHAR(1000) NULL DEFAULT NULL COMMENT '项目名称')"
    val ind1 = a1.indexOf("(")
    val ind2 = a1.indexOf(",")
    val str1 = a1.substring(ind1+1,ind2).trim
    val ind3 = str1.indexOf(" ")
    val str2 = str1.substring(0, ind3)

    println(s"field: $str2")

    val a2 = a1 + s""" CLUSTERED BY(${str2}) INTO 3 BUCKETS STORED BY ORC TBLPROPERTIES("TRANSACTIONAL"="true")"""

    println(a2)


  }

}
