#! /usr/bin/env bash

fpath="$1"
echo "$fpath "
tpath=hdfs://lscluster$2
echo "$tpath"
mydirr=$3
echo "mydir: $mydirr"
tabname=$4
tabname2=${tabname/\./\_}
fail_table=$mydirr/fail_table.txt

sed -i.bak 's/create [ ]*table/create external table/g' "$fpath"
sed -i '/^[[:space:]]*$/d' "$fpath"
sed -i '0,/\./s/\./\_/' $fpath

sed -i 's/character[ ][ ]*varying/varchar/g' "$fpath"
sed -i 's/character(/char(/g' "$fpath"
sed -i 's/[ ]text/ string/g' "$fpath"
sed -i 's/[ ]integer/ int/g' "$fpath"
sed -i 's/[ ]real/ float/g' "$fpath"
sed -i 's/double[ ][ ]*precision/double/g' "$fpath"
sed -i 's/[ ]numeric/ decimal/g' "$fpath"
sed -i 's/timestamp[ ][ ]*without[ ][ ]*time[ ][ ]*zone/string/g' "$fpath"

sed -i 'N;s/with[ ]*(.*//Ig' "$fpath"
sed -i "s#[)]*[ ]*distributed by[ ]*(.*#row format delimited fields terminated by '\\\005' location '$tpath';#g" "$fpath"

allsql=`cat $fpath`

#${mydirr}/dosql.exp "$allsql"
create_status=`expect /home/ods2hdfs/dosql.exp "$allsql" "$tabname2"`

echo "create table status: $create_status"
if [ $create_status == "no" ];then
  echo "$tabname2 create fail" >> $fail_table
fi
#echo " exit code $?"
