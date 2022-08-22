#!/bin/bash

APP=edu
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ] ;then
    do_date=$2
else 
    do_date=`date -d "-1 day" +%F`
fi

ads_exam_by_paper="
use edu;
insert overwrite table ${APP}.ads_exam_by_paper
select * from ${APP}.ads_exam_by_paper
union
select
   '$do_date' dt,
   recent_days,
   papar_id,
   avg(avg_score_1d),
   avg(avg_time_1d),
   sum(user_num_1d)
from ${APP}.dws_exam_exam_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days-1)
group by recent_days,papar_id;
"

ads_exam_by_course="
insert overwrite table ${APP}.ads_exam_by_course
select * from ${APP}.ads_exam_by_course
union
select
   '$do_date' dt,
   recent_days,
   course_id,
   avg(avg_score_1d),
   avg(avg_time_1d),
   sum(user_num_1d)
from ${APP}.dws_exam_course_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days-1)
group by recent_days,course_id;
"
ads_exam_by_papar_score="
insert overwrite table ${APP}.ads_exam_by_papar_score
select * from ${APP}.ads_exam_by_papar_score
union
select
   '$do_date' dt,
   recent_days,
   papar_id,
   score_step,
   count(*) user_num
from ${APP}.dws_exam_exam_score_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days-1)
group by recent_days,papar_id,score_step;
"

ads_exam_by_question="
insert overwrite table ${APP}.ads_exam_by_question
select * from ${APP}.ads_exam_by_question
union
select
   '$do_date' dt,
   recent_days,
   question_id,
   cast(right_num/write_num*100 as DECIMAL(16, 2)) right_rate
from ${APP}.dws_exam_question_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days-1);
"



ads_user_change="
insert overwrite table ${APP}.ads_user_change
select * from ${APP}.ads_user_change
union
select
    churn.dt,
    user_churn_count,
    user_back_count
from
(
    select
        '$do_date' dt,
        count(*) user_churn_count
    from ${APP}.dws_user_user_login_td
    where dt='$do_date'
    and login_date_last=date_add('$do_date',-7)
)churn
join
(
    select
        '$do_date' dt,
        count(*) user_back_count
    from
    (
        select
            user_id,
            login_date_last
        from ${APP}.dws_user_user_login_td
        where dt='$do_date'
    )t1
    join
    (
        select
            user_id,
            login_date_last login_date_previous
        from ${APP}.dws_user_user_login_td
        where dt=date_add('$do_date',-1)
    )t2
    on t1.user_id=t2.user_id
    where datediff(login_date_last,login_date_previous)>=8
)back
on churn.dt=back.dt;
"


ads_user_retention="
insert overwrite table ${APP}.ads_user_retention
select * from ${APP}.ads_user_retention
union
select
    '$do_date' dt,
    login_date_first create_date,
    datediff('$do_date',login_date_first) retention_day,
    sum(if(login_date_last='$do_date',1,0)) retention_count,
    count(*) new_user_count,
    cast(sum(if(login_date_last='$do_date',1,0))/count(*)*100 as decimal(16,2)) retention_rate
from
(
    select
        user_id,
        date_id login_date_first
    from ${APP}.dwd_user_register_inc
    where dt>=date_add('$do_date',-7)
    and dt<'$do_date'
)t1
join
(
    select
        user_id,
        login_date_last
    from ${APP}.dws_user_user_login_td
    where dt='$do_date'
)t2
on t1.user_id=t2.user_id
group by login_date_first;
"




ads_user_stats="
insert overwrite table ${APP}.ads_user_stats
select * from ${APP}.ads_user_stats
union
select
    '$do_date' dt,
    t1.recent_days,
    new_user_count,
    active_user_count
from
(
    select
        recent_days,
        sum(if(login_date_last>=date_add('$do_date',-recent_days+1),1,0)) active_user_count
    from ${APP}.dws_user_user_login_td lateral view explode(array(1,7,30)) tmp as recent_days
    where dt='$do_date'
    group by recent_days
)t1
join
(
    select
        recent_days,
        sum(if(date_id>=date_add('$do_date',-recent_days+1),1,0)) new_user_count
    from ${APP}.dwd_user_register_inc lateral view explode(array(1,7,30)) tmp as recent_days
    group by recent_days
)t2
on t1.recent_days=t2.recent_days;
"


ads_user_action="
insert overwrite table ${APP}.ads_user_action
select * from ${APP}.ads_user_action
union
select
    '$do_date' dt,
    home_count,
    course_detail_count,
    cart_count,
    order_count,
    payment_count
from
(
    select
        recent_days,
        sum(if(page_id='home',1,0)) home_count,
        sum(if(page_id='course_detail',1,0)) course_detail_count
    from ${APP}.dws_traffic_page_visitor_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
	where dt >= date_sub('$do_date',recent_days-1)
    and page_id in ('home','good_detail')
	group by recent_days
)page
join
(
    select
        recent_days,
        count(*) cart_count
    from ${APP}.dws_trade_user_cart_add_1d lateral view explode(array(1,7,30)) tmp as recent_days
	where dt >= date_sub('$do_date',recent_days-1)
	group by recent_days
)cart
on page.recent_days=cart.recent_days
join
(
    select
        recent_days,
        count(*) order_count
    from ${APP}.dws_trade_user_order_1d lateral view explode(array(1,7,30)) tmp as recent_days
    where dt >= date_sub('$do_date',recent_days-1)
	group by recent_days
)ord
on page.recent_days=ord.recent_days
join
(
    select
        recent_days,
        count(*) payment_count
    from ${APP}.dws_trade_user_payment_1d lateral view explode(array(1,7,30)) tmp as recent_days
    where dt >= date_sub('$do_date',recent_days-1)
	group by recent_days
)pay
on page.recent_days=pay.recent_days;
"




ads_new_order_user_stats="
insert overwrite table ${APP}.ads_new_order_user_stats
select * from ${APP}.ads_new_order_user_stats
union
select
    '$do_date' dt,
    no.recent_days,
    new_order_user_count,
    new_pay_user_count
from (
    select
        recent_days,
        sum(if(order_date_first>=date_add('$do_date',-recent_days+1),1,0)) new_order_user_count
    from dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
    where dt='$do_date'
    group by recent_days
) no
join (
    select
       recent_days,
       sum(if(dt >= date_add('$do_date', -recent_days + 1), 1, 0)) new_pay_user_count
    from dws_trade_user_payment_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
    where dt = '$do_date'
    group by recent_days
) np
on no.recent_days = np.recent_days;
"


ads_trade_by_old="
insert overwrite table ${APP}.ads_trade_by_old
select * from ${APP}.ads_trade_by_old
union
select
    '$do_date' dt,
    recent_days,
    old_step,
    count(*) user_num
from ${APP}.dws_trade_old_order_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days-1)
group by recent_days,old_step;
"



ads_traffic_stats_by_channel="
insert overwrite table ${APP}.ads_traffic_stats_by_channel
select * from ${APP}.ads_traffic_stats_by_channel
union
select
    '$do_date' dt,
    recent_days,
    channel,
    cast(count(distinct(mid_id)) as bigint) uv_count,
    cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
    cast(avg(page_count_1d) as bigint) avg_page_count,
    cast(count(*) as bigint) sv_count,
    cast(sum(if(page_count_1d=1,1,0))/count(*) as decimal(16,2)) bounce_rate
from ${APP}.dws_traffic_session_page_view_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_add('$do_date',-recent_days+1)
group by recent_days,channel;
"




ads_page_path="
insert overwrite table ${APP}.ads_page_path
select * from ${APP}.ads_page_path
union
select
    '$do_date' dt,
	recent_days,
    source,
    nvl(target,'null'),
    count(*) path_count
from
(
    select
		recent_days,
        concat('step-',rn,':',page_id) source,
        concat('step-',rn+1,':',next_page_id) target
    from
    (
        select
			recent_days,
            page_id,
            lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
            row_number() over (partition by session_id order by view_time) rn
        from ${APP}.dwd_traffic_page_view_inc lateral view explode(array(1,7,30)) tmp as recent_days
		where dt>=date_add('$do_date',-recent_days+1)
    )t1
)t2
group by recent_days,source,target;
"



ads_course_order_by_cate="
insert overwrite table ${APP}.ads_course_order_by_cate
select * from ${APP}.ads_course_order_by_cate
union
select '$do_date' dt,
       recent_days,
       category_id,
       category_name,
       order_num,
       order_user_count,
       order_price_amount
from
(
    select
        1 recent_days,
        category_id,
        category_name,
        sum(order_num_1d) order_num,
        count(distinct(user_id)) order_user_count,
           sum(final_amount_1d) order_price_amount
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by category_id, 1, category_name
    union all
    select
        recent_days,
        category_id,
        category_name,
        sum(order_num),
        count(distinct(if(order_num>0,user_id,null)))  order_user_count,
           sum(order_price_amount)
    from
    (
        select
            recent_days,
            user_id,
            category_id,
            category_name,
            case recent_days
                when 7 then order_num_7d
                when 30 then order_num_30d
            end order_num,
            case recent_days
        when 7 then final_amount_7d
                when 30 then final_amount_30d
            end order_price_amount
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by category_id, recent_days, category_name
)odr;
"
        
        
ads_course_order_by_subject="
insert overwrite table ${APP}.ads_course_order_by_subject
select * from ${APP}.ads_course_order_by_subject
union
select '$do_date' dt,
       recent_days,
       subject_id,
       subject_name,
       order_num,
       order_user_count,
       order_price_amount
from
(
    select
        1 recent_days,
        subject_id,
       subject_name,
        sum(order_num_1d) order_num,
        count(distinct(user_id)) order_user_count,
           sum(final_amount_1d) order_price_amount
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by subject_id, 1, subject_name
    union all
    select
        recent_days,
        subject_id,
       subject_name,
        sum(order_num),
        count(distinct(if(order_num>0,user_id,null)))  order_user_count,
           sum(order_price_amount)
    from
    (
        select
            recent_days,
            user_id,
           subject_id,
       subject_name,
            case recent_days
                when 7 then order_num_7d
                when 30 then order_num_30d
            end order_num,
            case recent_days
        when 7 then final_amount_7d
                when 30 then final_amount_30d
            end order_price_amount
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by subject_id, recent_days, subject_name
)odr;
"

ads_course_order_by_course="
insert overwrite table ${APP}.ads_course_order_by_course
select * from ${APP}.ads_course_order_by_course
union
select '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       order_num,
       order_user_count,
       order_price_amount
from
(
    select
        1 recent_days,
         course_id,
       course_name,
        sum(order_num_1d) order_num,
        count(distinct(user_id)) order_user_count,
           sum(final_amount_1d) order_price_amount
    from ${APP}.dws_trade_user_course_order_1d
    where dt='$do_date'
    group by course_id, 1, course_name
    union all
    select
        recent_days,
        course_id,
       course_name,
        sum(order_num),
        count(distinct(if(order_num>0,user_id,null)))  order_user_count,
           sum(order_price_amount)
    from
    (
        select
            recent_days,
            user_id,
            course_id,
       course_name,
            case recent_days
                when 7 then order_num_7d
                when 30 then order_num_30d
            end order_num,
            case recent_days
        when 7 then final_amount_7d
                when 30 then final_amount_30d
            end order_price_amount
        from ${APP}.dws_trade_user_course_order_nd lateral view explode(array(7,30)) tmp as recent_days
        where dt='$do_date'
    )t1
    group by course_id, recent_days, course_name
)odr;
"

ads_review_by_course="
insert overwrite table ${APP}.ads_review_by_course
select * from ${APP}.ads_review_by_course
union
select '$do_date' ,
       1 recent_days,
       course_id,
       total_score/review_num,
       review_num,
       review_stars_5/review_num
from ${APP}.dws_interaction_course_review_info_1d
where dt='$do_date'
union all
select '$do_date' ,
       recent_days,
       course_id,
       case recent_days
when 7 then total_score_7d/review_num_7d
when 30 then  total_score_30d/review_num_30d
end avg_star,
       case recent_days
when 7 then review_num_7d
when 30 then  review_num_30d
end review_num,
        case recent_days
when 7 then review_stars_5_7d/review_num_7d
when 30 then  review_stars_5_30d/review_num_30d
end great_judg_rate
from ${APP}.dws_interaction_course_review_info_nd lateral view explode(array(7,30))tmp as recent_days
where dt = '$do_date';
"
ads_study_finish_avg_by_course="
insert overwrite table ads_study_finish_avg_by_course
select '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       round(count(user_id)/count(distinct user_id),2) avg
from dws_study_chapter_finish_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt >= date_sub('$do_date',recent_days - 1) and dt <= '$do_date'
group by recent_days,course_id,course_name;
"
ads_study_finish_KPI="
insert overwrite table ads_study_finish_KPI
select '$do_date' dt,
       recent_days,
       count(distinct if(finish_count=total_count_by_course,user_id,null)) amount,
       count(if(finish_count=total_count_by_course,user_id,null)) person_time
from (
         select recent_days,
                user_id,
                finish_count,
                total_count_by_course
         from (
                  select user_id,
                         recent_days,
                         course_name,
                         course_id,
                         count(user_id) finish_count

                  from (
                           select user_id,
                                  recent_days,
                                  course_id,
                                  course_name,
                                  row_number() over (partition by user_id,video_id) rn
                           from dws_study_chapter_finish_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
                           where dt >= date_sub('$do_date', recent_days - 1)
                             and dt <= '$do_date'
                       ) t1
                  group by user_id, recent_days,course_id, course_name, rn
              ) t2
                  left join (
             select course_id,
                    count(1) total_count_by_course
             from dim_video_full
             where dt = '$do_date'
             group by course_id
         ) t3 on t2.course_id = t3.course_id
     ) t4
group by recent_days;
"
ads_study_finish_by_course="
insert overwrite table ads_study_finish_by_course
select  '$do_date' dt,
        recent_days,
        course_id,
        course_name,
        count(distinct if(finish_count>=total_count_by_course,user_id,null)) count
from (
         select user_id,
                recent_days,
                course_name,
                t1.course_id,
                finish_count,
                total_count_by_course
         from (
                  select user_id,
                         recent_days,
                         course_id,
                         course_name,
                         count(distinct course_id) finish_count
                  from dws_study_chapter_finish_1d lateral view explode(array(1, 7, 30)) tmp as recent_days
                  where dt >= date_sub('$do_date', recent_days-1)
                    and dt <= '$do_date'
                  group by course_id, course_name, recent_days, user_id
              ) t1
                  left join (
             select course_id,
                    count(1) total_count_by_course
             from dim_video_full
             where dt = '$do_date'
             group by course_id
         ) t2 on t1.course_id = t2.course_id
     )t3 group by course_name,course_id,recent_days;
"
ads_study_video_by_course="
insert overwrite table ads_study_video_by_course
select '$do_date' dt,
       recent_days,
       course_id,
       course_name,
       play_count,
       avg_time,
       user_count
from (
         select 1                                          recent_days,
                course_id,
                course_name,
                count(*)                                   play_count,
                sum(during_time) / count(distinct user_id) avg_time,
                count(distinct user_id)                    user_count
         from dws_study_user_chapter_video_play_1d
         where dt = '$do_date'
         group by course_id, course_name
         union all
         select recent_days,
                course_id,
                course_name,
                count(*)                                   play_count,
                sum(during_time) / count(distinct user_id) avg_time,
                count(distinct user_id)                    user_count
         from (
                  select user_id,
                         recent_days,
                         course_id,
                         course_name,
                         case recent_days
                             when 7 then during_time_7d
                             when 30 then during_time_30d
                             end during_time
                  from dws_study_user_chapter_video_play_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '$do_date'
              ) t1 group by course_id,course_name,recent_days
     ) t2;
"
ads_study_video_by_chapter="
insert overwrite table ads_study_video_by_chapter
select '$do_date' dt,
       recent_days,
       chapter_id,
       chapter_name,
       play_count,
       avg_time,
       user_count
from (
         select 1                                          recent_days,
                chapter_id,
                chapter_name,
                count(*)                                   play_count,
                sum(during_time) / count(distinct user_id) avg_time,
                count(distinct user_id)                    user_count
         from dws_study_user_chapter_video_play_1d
         where dt = '$do_date'
         group by chapter_id, chapter_name
         union all
         select recent_days,
                chapter_id,
                chapter_name,
                count(*)                                   play_count,
                sum(during_time) / count(distinct user_id) avg_time,
                count(distinct user_id)                    user_count
         from (
                  select user_id,
                         recent_days,
                         chapter_id,
                         chapter_name,
                         case recent_days
                             when 7 then during_time_7d
                             when 30 then during_time_30d
                             end during_time
                  from dws_study_user_chapter_video_play_nd lateral view explode(array(7, 30)) tmp as recent_days
                  where dt = '$do_date'
              ) t1 group by chapter_id,chapter_name,recent_days
     ) t2;
"
ads_trade_order_by_days="
insert overwrite table ads_trade_order_by_days
select '$do_date',
       recent_days,
        sum(if(dt >= date_sub('$do_date',recent_days-1),price,0)) total_price,
        count(if(dt >= date_sub('$do_date',recent_days-1),order_id,null)) total_order_amount,
        count(if(dt >= date_sub('$do_date',recent_days-1),user_id,0)) user_count
from dws_trade_order_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_sub('$do_date',29) and dt <='$do_date'
group by recent_days;
"
ads_trade_order_by_province="
insert overwrite table ads_trade_order_by_province
select '$do_date',
        recent_days,
        province_id,
        province_name,
        iso_3166_2,
        sum(price) total_price,
        count(order_id) total_order_amount,
        count(user_id) user_count
from dws_trade_order_1d lateral view explode(array(1,7,30)) tmp as recent_days
where dt>=date_sub('$do_date',recent_days-1) and dt <='$do_date'
group by recent_days,province_id,province_name,iso_3166_2;
"

ads_trade_video_retention_by_category="
insert overwrite table ads_trade_video_retention_by_category
select '$do_date',
       1 recent_days,
       dws_course_audition_order_1d.category_id,
       dws_course_audition_order_1d.category_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt = '$do_date'
group by category_id, category_name
union all
select '$do_date',
       7 recent_days,
       category_id,
       category_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt <= '$do_date' and dt >=date_sub('$do_date',6)
group by category_id, category_name;
"

ads_trade_video_retention_by_subject="
insert overwrite table ads_trade_video_retention_by_subject
select '$do_date',
       1 recent_days,
       dws_course_audition_order_1d.subject_id,
       dws_course_audition_order_1d.subject_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt = '$do_date'
group by subject_id, subject_name
union all
select '$do_date',
       7 recent_days,
       subject_id,
       subject_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt <= '$do_date' and dt >=date_sub('$do_date',6)
group by subject_id, subject_name;
"

ads_trade_video_retention_by_course="
insert overwrite table ads_trade_video_retention_by_course
select '$do_date',
       1 recent_days,
       course_id,
       course_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt = '$do_date'
group by course_id, course_name
union all
select '$do_date',
       7 recent_days,
       course_id,
       course_name,
       count(distinct user_id) people_count,
       round(count(order_id)/count(user_id),2) retention_rate
from dws_course_audition_order_1d
where dt <= '$do_date' and dt >=date_sub('$do_date',6)
group by course_id, course_name;
"
case $1 in
    "ads_exam_by_paper" )
        hive -e "$ads_exam_by_paper"
    ;;
    "ads_exam_by_course" )
        hive -e "$ads_exam_by_course"
    ;;
    "ads_exam_by_papar_score" )
        hive -e "$ads_exam_by_papar_score"
    ;;
    "ads_exam_by_question" )
        hive -e "$ads_exam_by_question"
    ;;
    "ads_user_change" )
        hive -e "$ads_user_change"
    ;;
    "ads_user_retention" )
        hive -e "$ads_user_retention"
    ;;
    "ads_user_stats" )
        hive -e "$ads_user_stats"
    ;;
    "ads_user_action" )
        hive -e "$ads_user_action"
    ;;
    "ads_new_order_user_stats" )
        hive -e "$ads_new_order_user_stats"
    ;;
    "ads_trade_by_old" )
        hive -e "$ads_trade_by_old"
    ;;
    "ads_traffic_stats_by_channel" )
        hive -e "$ads_traffic_stats_by_channel"
    ;;
    "ads_page_path" )
        hive -e "$ads_page_path"
    ;;
    
	
	"ads_course_order_by_cate" )
        hive -e "$ads_course_order_by_cate"
    ;;
    "ads_course_order_by_subject" )
        hive -e "$ads_course_order_by_subject"
    ;;
    "ads_course_order_by_course" )
        hive -e "$ads_course_order_by_course"
    ;;
    "ads_review_by_course" )
        hive -e "$ads_review_by_course"
    ;;
	
	
	
    "all" )
        hive -e "$ads_exam_by_paper$ads_exam_by_course$ads_exam_by_papar_score$ads_exam_by_question$ads_user_change$ads_user_retention$ads_user_stats$ads_user_action$ads_new_order_user_stats$ads_trade_by_old$ads_traffic_stats_by_channel$ads_page_path$ads_course_order_by_cate$ads_course_order_by_subject$ads_course_order_by_course$ads_review_by_course
		$ads_trade_video_retention_by_course
		$ads_trade_video_retention_by_subject
		$ads_trade_video_retention_by_category
		$ads_trade_order_by_province
		$ads_trade_order_by_days
		$ads_study_video_by_chapter
		$ads_study_video_by_course
		$ads_study_finish_by_course
		$ds_study_finish_KPI
		$ads_study_finish_avg_by_course
		"
    ;;
esac