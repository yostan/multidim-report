package com.yx.report.tests

import org.apache.spark.Partitioner

/**
 * Created by naonao on 2016/4/18
 */
class MyPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    val tup2 = key.asInstanceOf[Tuple2[Int, Int]]
    val p = tup2._1 % numPartitions
    p
  }
}
