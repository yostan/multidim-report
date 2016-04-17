#! /bin/bash

job="_job";
db="mysql59.";
while read line
  do
    jobname="${line}${job}";
    hivetable="${db}${line}";
    sqoop job --create $jobname -- import --connect jdbc:mysql://ip:3306/iodata --delete-target-dir \
          --username u --password p --table $line --hive-import --hive-table $hivetable -m 1;
    sqoop job --exec $jobname;
  done < /home/sqoop/test/tables.txt