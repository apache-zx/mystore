#!/bin/bash


APP=vof
hive=hive

drop_dwd_user_feature_result="DROP TABLE IF EXISTS $APP.dwd_user_feature_result;"
drop_dwd_good_feature_result="DROP TABLE IF EXISTS $APP.dwd_good_feature_result;"

case $1 in
"all"){
  $hive -e "$drop_dwd_user_feature_result\
            $drop_dwd_good_feature_result
           "
};;
esac
