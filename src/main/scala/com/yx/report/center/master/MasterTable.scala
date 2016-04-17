package com.yx.report.center.master

import com.yx.report.center.Table
import com.yx.report.utils.ReportConf

/**
 * Created by naonao on 2016/3/30
 */
class MasterTable(reportConf: ReportConf) extends Table {

  override val conf = reportConf.master_table


}
