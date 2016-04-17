package com.yx.report.center.slave2

import com.yx.report.center.Table
import com.yx.report.utils.{Tab, ReportConf}

/**
 * Created by naonao on 2016/3/30
 */
class Slave2Table(reportConf: ReportConf) extends Table{

  override val conf = reportConf.slave2_table

}
