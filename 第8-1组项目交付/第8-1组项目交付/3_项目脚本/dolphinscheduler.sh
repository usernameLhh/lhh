#! /bin/bash

case $1 in 
"start")
	echo "---------- dolphinscheduler $i 启动 ------------"
	`/opt/software/apache-dolphinscheduler-1.3.9-bin/bin/start-all.sh`
;;
"stop")
	echo "---------- dolphinscheduler $i 停止 ------------"
	`/opt/software/apache-dolphinscheduler-1.3.9-bin/bin/stop-all.sh`
;;
*)
	echo " --- 输入有误-----"
;;
esac
