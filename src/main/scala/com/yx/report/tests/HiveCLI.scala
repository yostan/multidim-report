package com.yx.report.tests

import scala.io.Source
import scala.sys.process._

/**
 * Created by naonao on 2016/5/11
 */
object HiveCLI {

  def main(args: Array[String]) {
    val fileBuf = Source.fromFile(args(2))
    val sb = new StringBuilder()
    for(line <- fileBuf.getLines()){
      sb ++= line
    }
    val sqlll = sb.toString()

    val cmd = Seq("hive", "--database", "oracletest", "-e", sqlll, ">", args(1))
    println("******** start cmd ************")
    val exitCode = cmd.!
    println(s"exit-code: $exitCode")
  }

}
