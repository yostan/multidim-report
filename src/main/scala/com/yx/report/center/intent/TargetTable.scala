package com.yx.report.center.intent

import com.yx.report.center.Table
import com.yx.report.utils.{TargetTab, ReportConf}

/**
 * Created by naonao on 2016/3/30
 */
class TargetTable(reportConf: ReportConf) extends Table {

  override val conf = reportConf.target_table

  private val gen_frequency: String = conf.generate_frequency

  def parseConf(conf: TargetTab): Unit = {
    println("test")
  }


}



object TargetTable {


}
