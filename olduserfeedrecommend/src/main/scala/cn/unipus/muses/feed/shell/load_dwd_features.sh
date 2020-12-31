#!/bin/bash

APP=vof
hive=hive

if [ -n "$2" ] ;then
	do_date=$2
else
	do_date=`date -d "-1 day" +%F`
fi

load_user_feature_result_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.dwd_user_feature_result
PARTITION (dt='$do_date')
select
t3.label,
t3.user_id,
t4.level,
t3.feature_count
from
(select
t1.user_id,
t1.label,
t2.feature_count
from
(select
user_id,
label
from dwd_user_look_label) t1
inner join (
select
user_id,
count(label) feature_count
from dwd_user_look_label
group by user_id
) t2
on t1.user_id = t2.user_id) t3
inner join (
select
id,
level
from dwd_base_user
where del_flag = '0'
) t4
on t4.id =t3.user_id
"

load_good_feature_result_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.dwd_good_feature_result
PARTITION (dt='$do_date')
select
t5.label,
t6.id,
t5.grade,
t5.feature_count
from
(select
t3.course_id,
t3.label,
t4.grade,
t3.feature_count
from
(select
t1.course_id,
t1.label,
t2.feature_count
from
(select
course_id,
label
from dwd_course_label
where del_flag = 'false') t1
inner join
(select
course_id,
count(label) feature_count
from dwd_course_label
where del_flag = 'false'
group by course_id) t2
on t1.course_id = t2.course_id) t3
inner join (
select
course_id,
grade
from dwd_course_grade
where del_flag = 'false'
) t4
on t3.course_id = t4.course_id) t5
inner join (
select
id,
third_party_id
from dwd_vof_goods
where status ='7'
and del_flag ='false'
) t6
on t5.course_id = t6.third_party_id
"

load_user_look_result_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.dwd_user_look_result
PARTITION (dt='$do_date')
select
t1.user_id,
concat_ws('_',collect_set(goods_id)) goods_list
from
(select
user_id,
goods_id
from dwd_user_look
group by user_id,goods_id) t1
group by t1.user_id
"

case $1 in
"all"){
    $hive -e "$load_user_feature_result_sql\
              $load_user_look_result_sql\
              $load_good_feature_result_sql
              "
};;
esac
