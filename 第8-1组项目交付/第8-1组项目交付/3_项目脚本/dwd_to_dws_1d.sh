#!/bin/bash
APP=edu

# 如果输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

dws_exam_exam_1d="
use edu;
insert overwrite table ${APP}.dws_exam_exam_1d partition(dt='$do_date')
select
   papar_id,
   avg(score) avg_score_1d,
   avg(duration_sec) avg_time_1d,
   count(*) user_num_1d
from ${APP}.dwd_exam_user_exam_inc
where dt='$do_date'
group by papar_id;
"
dws_exam_course_1d="
insert overwrite table ${APP}.dws_exam_course_1d partition(dt='$do_date')
select
   course_id,
   avg(score) avg_score_1d,
   avg(duration_sec) avg_time_1d,
   count(*) user_num_1d
from ${APP}.dim_exam_full
where dt='$do_date'
group by course_id;
"
dws_exam_exam_score_1d="
insert overwrite table ${APP}.dws_exam_exam_score_1d partition(dt='$do_date')
select
   distinct user_id,
   papar_id,
   if(score<60,'0-60',if(score<80,'60-80',if(score<90,'80-90','90-100'))) score_step
from ${APP}.dwd_exam_user_exam_inc
where dt='$do_date';
"

dws_exam_question_1d="
insert overwrite table ${APP}.dws_exam_question_1d partition(dt='$do_date')
select
   exam_question_id,
   sum(if(exam_question_is_correct='1',1,0)) right_num,
   count(*) write_num
from ${APP}.dim_exam_full
where dt='$do_date'
group by exam_question_id;
"
dws_trade_user_cart_add_1d="
insert overwrite table ${APP}.dws_trade_user_cart_add_1d partition(dt='$do_date')
select
    user_id,
    count(*)
from ${APP}.dwd_trade_cart_add_inc
where dt='$do_date'
group by user_id;
"

dws_trade_user_order_1d="
insert overwrite table ${APP}.dws_trade_user_order_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(origin_amount),
    sum(coupon_reduce),
    sum(final_amount)
from ${APP}.dwd_trade_order_detail_inc
where dt='$do_date'
group by user_id;
"

dws_trade_user_payment_1d="
insert overwrite table ${APP}.dws_trade_user_payment_1d partition(dt='$do_date')
select
    user_id,
    count(distinct(order_id)),
    sum(total_amount)
from ${APP}.dwd_trade_pay_detail_suc_inc
where dt='$do_date'
group by user_id;
"

dws_trade_old_order_1d="
insert overwrite table ${APP}.dws_trade_old_order_1d partition(dt='$do_date')
select
    distinct od.user_id,
    old_step
from
(
    select
        user_id,
        dt
    from ${APP}.dwd_trade_order_detail_inc
    where dt='$do_date'
)od
left join
(
  select
        id user_id,
        year (current_date ()) - year (birthday),
        if ((year (current_date ()) - year (birthday)<20), '20岁以下', if ((year (current_date ()) - year (birthday)<30), '20-30岁', if ((year (current_date ()) - year (birthday)<40), '30-40岁', if ((year (current_date ()) - year (birthday)<50), '40-50岁', '50岁以上')))) old_step
    from ${APP}.dim_user_zip
)sku
on od.user_id=sku.user_id;
"

dws_traffic_session_page_view_1d="
insert overwrite table ${APP}.dws_traffic_session_page_view_1d partition(dt='$do_date')
select
    session_id,
    mid_id,
    brand,
    model,
    operate_system,
    version_code,
    channel,
    sum(during_time),
    count(*)
from ${APP}.dwd_traffic_page_view_inc
where dt='$do_date'
group by session_id,mid_id,brand,model,operate_system,version_code,channel;
"


dws_trade_user_course_order_1d="
insert overwrite table ${APP}.dws_trade_user_course_order_1d partition(dt='$do_date')
select order_id,
       user_id,
       ord_de.course_id,
       course_name,
       subject_id,
       subject_name,
       category_id,
       category_name,
       province_id,
       order_num_1d,
       origin_price_1d,
       reduce_amount_1d,
       final_amount_1d
from (
    (
		select dt,
		   order_id,
		   user_id,
		   course_id,
				province_id,
				count(*) order_num_1d,
		   sum(nvl(origin_amount,0.0)) origin_price_1d,
		   sum(nvl(coupon_reduce,0.0)) reduce_amount_1d,
		   sum(nvl(final_amount,0.0)) final_amount_1d
		from ${APP}.dwd_trade_order_detail_inc
		where dt='$do_date'
		group by dt, order_id, user_id, course_id,province_id
    )ord_de 
	left join (
       select course_id,
       course_name,
       subject_id,
       subject_name,
       category_id,
       category_name
	from ${APP}.dim_course_full
	where dt ='$do_date'
    ) dim_cou 
	on ord_de.course_id = dim_cou.course_id
);
"

dws_interaction_course_review_info_1d="
insert overwrite table ${APP}.dws_interaction_course_review_info_1d partition(dt='$do_date')
select 
	course_id,
	5star_num,
	4star_num,
	3star_num,
	2star_num,
	1star_num,
    (5star_num*5)+(4star_num*4)+ (3star_num*3)+ (2star_num*2)+ (1star_num*1),
    review_num
from 
(
	select course_id,
	   sum(if(review_stars=5,1,0)) 5star_num,
	   sum(if(review_stars=4,1,0)) 4star_num,
	   sum(if(review_stars=3,1,0)) 3star_num,
	   sum(if(review_stars=2,1,0)) 2star_num,
	   sum(if(review_stars=1,1,0)) 1star_num,
	   count(distinct user_id) review_num
	from ${APP}.dwd_interaction_course_comment_inc
	where dt='$do_date'
	group by course_id
) star;
"
dws_course_audition_order_1d="
insert overwrite table dws_course_audition_order_1d partition (dt='$do_date')
select t1.user_id,
       t1.video_id,
       subject_id,
       subject_name,
       t2.course_id,
       course_name,
       chapter_id,
       chapter_name,
       category_id,
       category_name,
       order_id
from (
         select user_id,
                video_id,
                first_time
         from dwd_study_video_per_day_inc
         where dt = '$do_date'
     ) t1  join (
         select id,
                subject_id,
                subject_name,
                course_id,
                course_name,
                chapter_id,
                chapter_name,
                category_id,
                category_name
         from dim_video_full
         where dt = '$do_date'
    ) t2 on t1.video_id = t2.id
left join (
        select user_id,
               order_id,
               course_id
        from dwd_trade_order_detail_inc
        where dt='$do_date'
    ) t3 on t1.user_id = t3.user_id and t2.course_id=t3.course_id
    where concat(t1.user_id,t1.video_id) not in (
    select distinct concat(user_id,video_id)
    from dwd_study_video_per_day_inc
    where dt < '$do_date'
    );
"

dws_trade_order_1d="
insert overwrite table dws_trade_order_1d partition (dt='$do_date')
select order_id,
       user_id,
       create_time,
       t1.province_id,
       province_name,
       iso_3166_2,
       price
from (
     select order_id,
            user_id,
            create_time,
            province_id,
            final_amount price
    from dwd_trade_order_detail_inc
    where dt='$do_date'
         ) t1  join (
             select id,
                    province_name,
                    iso_3166_2
             from dim_province_full
             where dt='$do_date'
    ) t2 on t1.province_id = t2.id;
"

dws_study_chapter_finish_1d="
insert overwrite table dws_study_chapter_finish_1d partition (dt='$do_date')
select user_id,
       course_id,
       course_name,
       chapter_id,
       chapter_name,
       video_id
from (
         select user_id,
                video_id
         from dwd_study_user_chapter_process
         where dt = '$do_date'
     ) t1 join (
         select id,
                course_id,
                course_name,
                chapter_id,
                chapter_name
         from dim_video_full
         where dt='$do_date'
    ) t2 on t1.video_id = t2.id ;
"
dws_study_user_chapter_video_play_1d="
insert overwrite table dws_study_user_chapter_video_play_1d partition (dt='$do_date')
select user_id,
       course_id,
       course_name,
       chapter_id,
       chapter_name,
       category_id,
       category_name,
       during_time
from (
         select user_id,
                video_id,
                total_time during_time
         from dwd_study_video_per_day_inc
         where dt = '$do_date'
     ) t1 join (
         select id,
                course_id,
                course_name,
                chapter_id,
                chapter_name,
                category_id,
                category_name
         from dim_video_full
         where dt='$do_date'
    ) t2 on t1.video_id = t2.id;
"


case $1 in
    "dws_exam_exam_1d" )
        hive -e "$dws_exam_exam_1d"
    ;;
    "dws_exam_course_1d" )
        hive -e "$dws_exam_course_1d"
    ;;
    "dws_exam_exam_score_1d" )
        hive -e "$dws_exam_exam_score_1d"
    ;;
    "dws_exam_question_1d" )
        hive -e "$dws_exam_question_1d"
    ;;
    "dws_trade_user_cart_add_1d" )
        hive -e "$dws_trade_user_cart_add_1d"
    ;;
    "dws_trade_user_order_1d" )
        hive -e "$dws_trade_user_order_1d"
    ;;
    "dws_trade_user_payment_1d" )
        hive -e "$dws_trade_user_payment_1d"
    ;;
    "dws_trade_old_order_1d" )
        hive -e "$dws_trade_old_order_1d"
    ;;
    "dws_traffic_session_page_view_1d" )
        hive -e "$dws_traffic_session_page_view_1d"
    ;;
	  "dws_trade_user_course_order_1d" )
        hive -e "$dws_trade_user_course_order_1d"
    ;;
	  "dws_interaction_course_review_info_1d" )
        hive -e "$dws_interaction_course_review_info_1d"
    ;;
    "all" )
        hive -e "$dws_exam_exam_1d$dws_exam_course_1d$dws_exam_exam_score_1d$dws_exam_question_1d$dws_trade_user_cart_add_1d$dws_trade_user_order_1d$dws_trade_user_payment_1d$dws_trade_old_order_1d$dws_traffic_session_page_view_1d$dws_trade_user_course_order_1d$dws_interaction_course_review_info_1d$dws_course_audition_order_1d$dws_trade_order_1d$dws_study_chapter_finish_1d$dws_study_user_chapter_video_play_1d"
    ;;
esac
