#!/usr/bin/env bash

#tablename=summary_gz.test_201604
#tablename=summary_gz.test
#out_gpsql_path=/home/ods/sh/tmp/${tablename}_gp.sql

if [ $# != 2 ];then
  echo "Usage: $0 table_name importpath"
  echo "example: $0 summary_gz.test /data/ods_data"
  exit 1
fi

tablename=$1
importpath=$2

mypathh="$(cd $(dirname ${0});pwd)"

tabname1=`echo ${tablename}|awk -F. '{print $1}'`
tabname2=`echo ${tablename}|awk -F. '{print $2}'`
echo "tabname1: $tabname1"
echo "tabname2: $tabname2"

tabname3=`echo ${tabname1}|awk -F_ '{print $2}'`
tabname4=`echo ${tabname2}|awk -F_ '{print $2}'`
if [[ "${tabname3}" = "szx" || "${tabname3}" = "metadata" ]];then
    tabname3="GD"
elif [[ "${tabname3}" = "xx" ]];then
    tabname3="GZ"
else
    tabname3=`echo ${tabname3}|tr a-z A-Z`
fi
echo "tabname3: $tabname3"
echo "tabname4: $tabname4"

if [ "$tabname4" = "" ];then
tabname4="201604"
fi

#hdfs_path=/ods2hdfs/ods2hdfs/gpbackup/GZ/201604/summary_gz/dat
hdfs_path_prefix=/ods2hdfs/ods2hdfs/gpbackup/${tabname3}/${tabname4}/${tabname1}
hdfs_path=$hdfs_path_prefix/dat
hdfs_sql_path=$hdfs_path_prefix/sql
echo "hdfs_path: $hdfs_path"
echo "hdfs_path: $hdfs_sql_path"

out_txtgz_path=${importpath}/${tabname3}/output/${tabname2}.txt.gz
#out_gpsql_path=/home/ods/sh/tmp/${tablename}_gp.sql
out_gpsql_path=${importpath}/${tabname3}/output/${tablename}_gp.sql
echo "out_txtgz_path: $out_txtgz_path"
echo "out_gpsql_path: $out_gpsql_path"

out_hdfs_txtgz_path=${hdfs_path}/${tabname2}.txt.gz
out_hdfs_gpsql_path=${hdfs_sql_path}/${tablename}_gp.sql

hdfs dfs -test -f $out_hdfs_txtgz_path
if [ $? -eq 0 ];then
    hdfs dfs -rm $out_hdfs_txtgz_path
    echo "***** removed hdfs dir: ${out_hdfs_txtgz_path} *****"
fi

hdfs dfs -test -f $out_hdfs_gpsql_path
if [ $? -eq 0 ];then
    hdfs dfs -rm $out_hdfs_gpsql_path
    echo "***** removed hdfs dir: ${out_hdfs_gpsql_path} *****"
fi

echo "call transql.sh start ..."
sh $mypathh/transql.sh "$out_gpsql_path" "$hdfs_path" "$mypathh" "$tablename"
echo "call transql.sh finish ..."

hdfs dfs -test -e $hdfs_sql_path
if [ $? -ne 0 ];then
    hdfs dfs -mkdir -p $hdfs_sql_path
    echo "************ hdfs mkdir: ${hdfs_sql_path} **************"
fi

hdfs dfs -test -e $hdfs_path
if [ $? -ne 0 ];then
    hdfs dfs -mkdir -p $hdfs_path
    echo "************ hdfs mkdir: ${hdfs_path} **************"
fi

hdfs_success=no
hdfs dfs -put $out_txtgz_path $hdfs_path && hdfs dfs -put $out_gpsql_path $hdfs_sql_path
if [ $? -eq 0 ];then
    hdfs_success=yes
fi
echo "hdfs_success: $hdfs_success"

hdfs_txtgz_size=`hdfs dfs -du $out_hdfs_txtgz_path |awk -F ' ' '{print $1}'`
hdfs_gpsql_size=`hdfs dfs -du $out_hdfs_gpsql_path |awk -F ' ' '{print $1}'`
echo "hdfs_txtgz_size: $hdfs_txtgz_size"
echo "hdfs_gpsql_size: $hdfs_gpsql_size"

