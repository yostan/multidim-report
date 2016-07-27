#! /usr/bin/env bash

# 请提前配置好ssh免密码登录

ganglia_path="/usr/local/ganglia/"
ganglia_conf="/etc/ganglia/"
ganglia_boot="/etc/rc.d/init.d/gmond"
user_name="ods"
hosts_file="/home/${user_name}/hosts.txt"

while read line
  do
    scp -r $ganglia_path $user_name@$line:$ganglia_path
    scp -r $ganglia_conf $user_name@$line:$ganglia_conf
    scp $ganglia_boot $user_name@$line:$ganglia_boot
    ssh $user_name@$line "chkconfig --add gmond; chkconfig gmond on; service gmond start"
  done < $hosts_file