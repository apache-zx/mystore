#!/bin/bash

APP=vof
hive=hive

#创建hive和hbase关联关系表,年级和商品的对应关系表
create_ads_grade_goods_recently_sql="
CREATE  TABLE IF NOT EXISTS $APP.ads_grade_goods_recently(
grade String COMMENT '商品对应的年级',
grade_goods String COMMENT '对应年级的商品列表'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('"hbase.columns.mapping"' = '":key,info:grade_goods"')
TBLPROPERTIES('"hbase.table.name"'= '"vof:grade_goods_recently"');
"

#创建hive和hbase关联关系表,热门商品
create_ads_hot_goods_recently_sql="
CREATE  TABLE IF NOT EXISTS $APP.ads_hot_goods_recently(
grade String COMMENT '商品对应的年级',
hot_goods String COMMENT '热门商品列表'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('"hbase.columns.mapping"' = '":key,info:hot_goods"')
TBLPROPERTIES('"hbase.table.name"' = '"vof:hot_goods_recently"');
"

#创建hive和hbase关联关系表,新品商品
create_ads_new_goods_recently_sql="
CREATE  TABLE IF NOT EXISTS $APP.ads_new_goods_recently(
grade String COMMENT '商品对应的年级',
new_goods String COMMENT '热门商品列表'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('"hbase.columns.mapping"'= '":key,info:new_goods"')
TBLPROPERTIES('"hbase.table.name"' = '"vof:new_goods_recently"');
"

#创建hive和hbase关联关系表,用户浏览过商品列表
create_ads_user_looked_goods_sql="
CREATE  TABLE IF NOT EXISTS $APP.ads_user_looked_goods(
user_id String COMMENT '用户id',
goods_list String COMMENT '用户浏览的商品列表'
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES('"hbase.columns.mapping"'= '":key,info:user_looked"')
TBLPROPERTIES('"hbase.table.name"' = '"vof:user_looked_goods"');
"

case $1 in
"all"){
  $hive -e "$create_ads_user_looked_goods_sql\
            $create_ads_new_goods_recently_sql\
            $create_ads_hot_goods_recently_sql\
            $create_ads_grade_goods_recently_sql
           "
};;
esac