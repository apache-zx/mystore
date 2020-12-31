#!/bin/bash

APP=vof
hive=hive

if [ -n "$2" ] ;then
	do_date=$2
else
	do_date=`date -d "-1 day" +%F`
fi

#加载对应年级的商品sql
load_ads_grade_goods_recently_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.ads_grade_goods_recently
select
t3.grade,
concat_ws('_',collect_set(t3.id)) new_goods
from
(select
t1.id,
t2.grade
from
(select
id,
third_party_id
from dwd_vof_goods
where del_flag = 'false'
and status = '7') t1
inner join (
select
course_id,
grade
from dwd_course_grade
where del_flag = 'false'
) t2
on t1.third_party_id = t2.course_id
group by t1.id,t2.grade) t3
group by t3.grade
"
#加载新商品sql
load_ads_new_goods_recently_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.ads_new_goods_recently
select
t3.grade,
concat_ws('_',collect_set(t3.id)) new_goods
from
(select
t1.id,
t2.grade
from
(select
id,
third_party_id
from dwd_vof_goods
where datediff(CURRENT_TIMESTAMP ,create_time)<=30
and del_flag = 'false'
and status = '7') t1
inner join (
select
course_id,
grade
from dwd_course_grade
where del_flag = 'false'
) t2
on t1.third_party_id = t2.course_id
group by t1.id,t2.grade) t3
group by t3.grade
"
#加载用户浏览商品的sql
load_ads_user_looked_goods_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.ads_user_looked_goods
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
#加载热门商品的sql
load_ads_hot_goods_recently_sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.ads_hot_goods_recently
select
t7.grade,
concat_ws('_',collect_set(id)) as hot_goods
from
(select
t3.id,
t6.grade,
t3.dif_like,
t3.dif_access,
t3.dif_sales
from
(select
t1.id,
t1.like_count - t2.like_count as dif_like,
t1.access_count - t2.access_count as dif_access,
t1.sales_count - t2.sales_count as dif_sales
from
(select
id,
like_count,
access_count,
sales_count
from dwd_vof_goods_ext
where dt = '$do_date'
and del_flag = 'false') t1
inner join(
select
id,
like_count,
access_count,
sales_count
from dwd_vof_goods_ext
where dt = 'date_sub('$do_date',7)'
and del_flag = 'false'
) t2
on t1.id = t2.id) t3
)inner join (
select
t4.id,
t5.grade
from
(select
id,
third_party_id
from dwd_vof_goods
where del_flag = 'false'
and status = '7') t4
inner join (
select
course_id,
grade
from dwd_course_grade
where del_flag = 'false'
) t5
on t4.third_party_id = t5.course_id
group by t4.id,t5.grade
) t6
on t3.id = t6.id) t7
group by t7.grade
order by t3.dif_like,t3.dif_access,t3.dif_sales
"

case $1 in
"all"){
    $hive -e "$load_ads_hot_goods_recently_sql\
              $load_ads_user_looked_goods_sql\
              $load_ads_new_goods_recently_sql\
              $load_ads_grade_goods_recently_sql
              "
};;
esac