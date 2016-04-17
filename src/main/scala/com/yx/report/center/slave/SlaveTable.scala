package com.yx.report.center.slave

import com.yx.report.center.Table
import com.yx.report.utils.ReportConf

/**
 * Created by naonao on 2016/3/30
 */
class SlaveTable(reportConf: ReportConf) extends Table {

  override val conf = reportConf.slave_table
}
