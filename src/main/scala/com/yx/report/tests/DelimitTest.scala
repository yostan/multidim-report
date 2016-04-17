package com.yx.report.tests

import org.apache.spark.{SparkContext, SparkConf}

import scala.io.Source

/**
 * Created by naonao on 2016/3/24
 */
object DelimitTest {

  def main(args: Array[String]) {
    if(args.length < 1){
      System.err.println("Usage: DelimitTest <inputDataDir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("DelimitTest")//.setMaster("local[2]")
    //conf.set("spark.testing.reservedMemory","0")   //only local test
    val sc = new SparkContext(conf)

//    val fileBuf = Source.fromFile("src/main/resources/delimit.txt")
    val fileBuf = Source.fromInputStream(getClass.getResourceAsStream("/delimit.txt"))
    val sb = new StringBuilder()
    for(line <- fileBuf.getLines()){
      sb ++= line
    }
    val delimit = sb.toString()
    println(s"delimit: ************${delimit}*****************")

    val rdd1 = sc.textFile(args(0))

//    //rdd1.foreach(println)
    val rdd2 = rdd1.map(_.split(delimit)).map(x => (x(1),x(2),x(3),x(4)))
    rdd2.take(5).foreach(println)

    sc.stop()
  }

}
