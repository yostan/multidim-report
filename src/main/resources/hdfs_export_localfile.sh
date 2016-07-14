#!/usr/bin/env bash

#if [ $# -ne 2 ];then
#    echo "export hdfs file to local"
#    echo "Usage: $0 tablename out_path"
#    exit 1
#fi
#tablename=summary_gz.test_201604
#out_path=/home/ods/hadoop
tablename=$1
out_path=$2

tabname1=`echo ${tablename}|awk -F. '{print $1}'`
tabname2=`echo ${tablename}|awk -F. '{print $2}'`
echo tabname1: $tabname1
echo tabname2: $tabname2

tabname3=`echo ${tabname1}|awk -F_ '{print $2}'`
tabname4=`echo ${tabname2}|awk -F_ '{print $2}'`
if [[ "${tabname3}" = "szx" || "${tabname3}" = "metadata" ]];then
    tabname3="GD"
elif [[ "${tabname3}" = "xx" ]];then
    tabname3="GZ"
else
    tabname3=`echo ${tabname3}|tr a-z A-Z`
fi
echo tabname3: $tabname3
echo tabname4: $tabname4

hdfs_path=/data/ods_data/${tabname3}/${tabname4}/${tabname1}
echo hdfs_path: $hdfs_path

out_hdfs_txtgz_path=${hdfs_path}/${tabname2}.txt.gz
out_hdfs_gpsql_path=${hdfs_path}/${tablename}_gp.sql
out_local_path=${out_path}/${tabname3}/output

if [ ! -d $out_local_path ];then
    mkdir -p $out_local_path
    echo "***** created local dir: $out_local_path *****"
fi

hdfs_success=no
hdfs dfs -get $out_hdfs_txtgz_path $out_hdfs_gpsql_path $out_local_path
if [ $? -eq 0 ];then
    hdfs_success=yes
fi
echo hdfs_success: $hdfs_success