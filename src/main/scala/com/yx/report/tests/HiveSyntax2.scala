package com.yx.report.tests

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by naonao on 2016/3/29
 */
object HiveSyntax2 {

  def main(args: Array[String]) {
    if(args.length < 1){
      System.err.println("Usage: HiveSyntax2 <databaseName.tableName>")
      System.exit(1)
    }

    val conf = new SparkConf().setAppName("HiveSyntax2")
    val sc = new SparkContext(conf)
    val hc = new HiveContext(sc)

    val sql = s"""create table ${args(0)} AS
          select a.SERV_ID,
            a.ACC_NBR,
            COALESCE(e.ACC_NBR2, '-1'),
            a.CUST_ID,
            a.SERV_NBR,
            a.STATE,
            COALESCE(a.SERV_CHANNEL_ID, '-1'),
            COALESCE(a.SERV_STAT_ID, '-1'),
            COALESCE(a.USER_TYPE, '-1'),
            COALESCE(a.USER_CHAR, '-1'),
            COALESCE(a.PAYMENT_TYPE, '-1'),
            a.BILLING_TYPE_ID,
            a.PROD_ID,
            a.EXCHANGE_ID,
            a.SERV_COL1,
            a.SERV_COL2,
            COALESCE(a.STOP_TYPE, '-1'),
            a.CREATE_DATE,
            a.ADDRESS_ID,
            a.SUBS_DATE,
            a.OPEN_DATE,
            a.MODI_STAFF_ID,
            a.SALES_TYPE_ID,
            a.SERV_ADDR_ID,
            a.ADD_NAME,
            a.DEF_BM_DATE,
            a.USE_BM_DATE,
            a.BM_ORG_ID,
            d.PROD_CAT_ID,
            case c.SBRANCH_ORG when -1 then -cast(c.CITY_ID as bigint)
                               else c.SBRANCH_ORG
                               end BOARD_SUBST_ID,
            case c.SMANAGE_ORG when -1 then -abs(case c.SBRANCH_ORG when -1 then -cast(c.CITY_ID as bigint)
                                                                    else c.SBRANCH_ORG
                                                                    end)
                               else c.SMANAGE_ORG
                               end BOARD_BRANCH_ID,
            COALESCE(b.CCENTER, -1),
            b.BRANCH_ORG,
            b.MANAGE_ORG,
            case c.SBRANCH_ORG when -1 then -cast(c.CITY_ID as bigint)
                               else c.SBRANCH_ORG
                               end STD_SUBST_ID,
            case c.SMANAGE_ORG when -1 then -abs(case c.SBRANCH_ORG when -1 then -cast(c.CITY_ID as bigint)
                                                                                  else c.SBRANCH_ORG
                                                                                  end)
                                else c.SMANAGE_ORG
                                end STD_BRANCH_ID,
            c.BRANCH_ORG GIVENUM_ORG,
            c.BRANCH_ORG INCOME_ORG,
            c.MANAGE_ORG GIVENUM_SERVCEN_ORG,
            c.MANAGE_ORG INCOME_SERVCEN_ORG,
            c.CCENTER,
            f.SALES_ID,
            a.EXT_PROD_ID,
            a.PAYMENT_MODE_CD,
            a.STATUS_CD,
            floor(cast(datediff(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), a.USE_BM_DATE) AS decimal(6)) / 31) STOP_TIME,
            case year(a.OPEN_DATE) when substr(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss'), 1),1,4) then 0
                                   else 1
                                   end CZ_FLAG,
            (case
            when a.PROD_ID in (3204, 3205) and
            a.CREATE_DATE > date_format('2008-10-01', 'yyyy-MM-dd') then
            2
            when a.PROD_ID in (3204, 3205) then
            1
            else
            -1
            end) CDMA_CLASS_ID,
            floor(months_between(date_format(from_unixtime(unix_timestamp()),'yyyy-MM-dd'),
            date_format(COALESCE(a.OPEN_DATE,
            a.CREATE_DATE),
            'yyyy-MM-dd'))) ONLINE_TIME,
            COALESCE((case
            when a.PROD_ID in (3204, 3205) and
            substr(a.ACC_NBR, 1, 3) in
            ('133', '153', '189', '180', '181', '177') then
            cast(substr(acc_nbr, 1, 3) AS decimal(4))
            end),
            -1) PHONE_NUMBER_ID
              from oracle23.TB_CM_SERV a
                  LEFT JOIN oracle23.TB_MO_GRID_INCOMEORG b on a.SERV_ID = b.SERV_ID
                  LEFT JOIN oracle23.TB_MO_SERV_INCOMEORG c on a.SERV_ID = c.SERV_ID
                  LEFT JOIN oracle23.TB_PM_PRODUCT d on a.PROD_ID = d.PROD_ID
                                                      and d.city_id = '0'
                  LEFT JOIN oracle23.TB_PRE_CM_DIAL e on a.SERV_ID = e.SERV_ID
                  LEFT JOIN oracle23.TB_PRE_CM_SALES f on a.SERV_ID = f.SERV_ID"""

    val startTime = System.currentTimeMillis()
    hc.sql(sql)
    val totalTime = startTime - System.currentTimeMillis()
    println(s"************* total Time 2: $totalTime *************")

    sc.stop()
  }

}
