#!/bin/bash

APP=vof
hive=hive

#用户维度表
create_dwd_user_feature_result="
CREATE EXTERNAL TABLE IF NOT EXISTS $APP.dwd_user_feature_result(
label string  COMMENT '用户维度标签',
user_id string COMMENT '用户id',
level string  COMMENT '用户年级',
feature_count bigint COMMENT '用户标签维度数'
) COMMENT '用户维度表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"

#商品维度表
create_dwd_good_feature_result="
CREATE EXTERNAL TABLE IF NOT EXISTS $APP.dwd_good_feature_result(
label string  COMMENT '商品维度标签',
good_id string COMMENT '商品id',
level string  COMMENT '商品对应年级',
feature_count bigint COMMENT '商品标签维度数'
) COMMENT '商品维度表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"

#用户浏览商品表
create_dwd_user_look_result="
CREATE EXTERNAL TABLE IF NOT EXISTS $APP.dwd_user_look_result(
user_id string  COMMENT '用户id',
goods_list string COMMENT '用户浏览'
) COMMENT '用户历史浏览表'
PARTITIONED BY (dt string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
"

case $1 in
"all"){
  $hive -e "$create_dwd_user_feature_result\
            $create_dwd_user_look_result\
            $create_dwd_good_feature_result
           "
};;
esac


