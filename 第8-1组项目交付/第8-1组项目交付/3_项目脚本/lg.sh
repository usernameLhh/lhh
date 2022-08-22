#!/bin/bash
for i in hadoop103 hadoop104
do
 echo "============== $i ===================="
 ssh $i "cd /opt/module/data_mocker;java -jar edu2021-mock-2022-06-18.jar"
done
