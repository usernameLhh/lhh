
#!/bin/bash
APP=edu

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_trade_user_course_order_nd="
use edu;
insert overwrite table ${APP}.dws_trade_user_course_order_nd partition(dt='$do_date')
select order_id,
       user_id,
       course_id,
       course_name,
       subject_id,
       subject_name,
       category_id,
       category_name,
       province_id,
       sum(if(dt >= date_sub('$do_date', 6), order_num_1d, 0))     order_num_7d,
       sum(if(dt >= date_sub('$do_date', 6), origin_price_1d, 0))  origin_price_7d,
       sum(if(dt >= date_sub('$do_date', 6), reduce_amount_1d, 0)) reduce_amount_7d,
       sum(if(dt >= date_sub('$do_date', 6), final_amount_1d, 0))  final_amount_7d,
       sum(order_num_1d),
       sum(origin_price_1d),
       sum(reduce_amount_1d),
       sum(final_amount_1d)
from ${APP}.dws_trade_user_course_order_1d
where dt >= date_sub('$do_date', 29)
group by order_id, user_id, course_id, course_name, subject_id, subject_name, category_id, category_name,province_id;
"

dws_study_user_chapter_video_play_nd="
insert overwrite table dws_study_user_chapter_video_play_nd partition (dt='$do_date')
select user_id,
       course_id,
       course_name,
       chapter_id,
       chapter_name,
       category_id,
       category_name,
       sum(if(recent_day = 7,during_time,0)) during_time_7d,
       sum(if(recent_day = 30,during_time,0)) during_time_30d
from dws_study_user_chapter_video_play_1d lateral view explode(array(7,30)) tmp as recent_day
where dt >= date_sub('$date',recent_day - 1 )
group by user_id, course_id, course_name, chapter_id, chapter_name, category_id, category_name;
"
case $1 in
    "dws_trade_user_course_order_nd" )
        hive -e "$dws_trade_user_course_order_nd"
    ;;
    "all" )
        hive -e "$dws_trade_user_course_order_nd$dws_study_user_chapter_video_play_nd"
    ;;
esac