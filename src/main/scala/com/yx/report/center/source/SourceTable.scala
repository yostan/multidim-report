package com.yx.report.center.source

import com.yx.report.center.Table
import com.yx.report.utils.ReportConf

/**
 * Created by naonao on 2016/3/30
 */
class SourceTable(reportConf: ReportConf) extends Table {

  override val conf = reportConf.source_table


}
