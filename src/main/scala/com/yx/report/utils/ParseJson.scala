package com.yx.report.utils

import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.io.Source

/**
 * Created by naonao on 2016/3/29
 */

sealed trait Tab
case class TargetTab(account: String, table_name: String, table_fields: List[String], primary_key: String, generate_frequency: String) extends Tab
case class SourceTab(tablename_fieldname: List[String]) extends Tab
case class MasterTab(account: String, table_name: String, where_condition: String, is_distinct: String) extends Tab
case class SlaveTab(account: String, tables: List[JoinCondition]) extends Tab
case class Slave2Tab(account: String, tables: List[JoinCondition]) extends Tab
case class JoinCondition(table_name: String, join_mode: String, join_condition: String, where_condition: String, join_fields: List[String], is_distinct: String)
case class ReportConf(target_table: TargetTab, source_table: SourceTab, master_table: MasterTab, slave_table: SlaveTab, slave2_table: Slave2Tab)



object ParseJson {

  def extractConf(args: Array[String]): ReportConf = {
    val fileBuf = Source.fromInputStream(getClass.getResourceAsStream("/report_conf.json")).mkString
    val j = parse(fileBuf)
    implicit val formats = DefaultFormats
    j.extract[ReportConf]
  }

}
