#!/bin/bash

APP=edu

if [ -n "$2" ] ;then
	do_date=$2
else
	do_date=`date -d "-1 day" +%F`
fi

dwd_interaction_chapter_comment_inc="
use edu;
insert overwrite table dwd_interaction_chapter_comment_inc partition(dt='$do_date')
select
    data.id,
    data.user_id,
    data.chapter_id,
    data.course_id,
    data.comment_txt,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods_comment_info_inc
where dt='$do_date'
and type = 'insert';
"
dwd_interaction_course_comment_inc="
insert overwrite table dwd_interaction_course_comment_inc partition(dt='$do_date')
select
    data.id,
    data.user_id,
    data.course_id,
    data.review_txt,
    data.review_stars,
    date_format(data.create_time,'yyyy-MM-dd') date_id,
    data.create_time
from ods_review_info_inc
where dt='$do_date'
and type = 'insert';
"
dwd_interaction_favor_add_inc="
insert overwrite table dwd_interaction_favor_add_inc partition(dt='$do_date')
select
   data.id,
   data.user_id,
   data.course_id,
   date_format(data.create_time,'yyyy-MM-dd') date_id,
   data.create_time,
   data.update_time,
   data.deleted
from ods_favor_info_inc
where dt='$do_date'
and type = 'insert';
"
dwd_study_user_chapter_process="
insert overwrite table dwd_study_user_chapter_process partition (dt)
select user_id,
       video_id,
       sum(time_acc)             time_acc,
       max(position_sec)         posititon_sec,
       min(first_time)           first_time,
       max(last_time)            last_time,
       min(acc_finish_date)      acc_finish_date,
       min(position_finish_date) position_finish_date,
       min(finish_time)          finish_time,
       min(finish_time)          finish_time

from (
         select user_id,
                video_id,
                time_acc,
                position_sec,
                first_time,
                last_time,
                acc_finish_date,
                position_finish_date,
                finish_time
         from dwd_study_user_chapter_process
         where dt = '9999-12-31'
         union all
         select *,
                if(acc_finish_date = '9999-12-31' or position_finish_date = '9999-12-31', '9999-12-31',
                   '$do_date') finish_date
         from (
                  select distinct user_id,
                                  vi.id                        video_id,
                                  time_acc,
                                  cast(position_sec as bigint) position_sec,
                                  first_time,
                                  last_time,
                                  if(time_acc  >= cast(during_sec as bigint) * 0.9, last_time,
                                     '9999-12-31')             acc_finish_date,
                                  if(position_sec >= cast(during_sec as bigint) * 0.9, last_time,
                                     '9999-12-21')             position_finish_date
                  from (
                           select common.uid                                 user_id,
                                  appvideo.video_id,
                                  sum(appvideo.play_sec)                     time_acc,
                                  max(cast(appvideo.position_sec as bigint)) position_sec,
                                  '$do_date'                               first_time,
                                  '$do_date'                               last_time
                           from ods_log_inc
                           where dt = '$do_date'
                             and appvideo.video_id is not null
                           group by common.uid, appvideo.video_id
                       ) process
                           join ods_video_info_full vi on process.video_id = vi.id
              ) t
     ) tmp
group by user_id, video_id;
"
dwd_study_video_per_day_inc="
insert overwrite table dwd_study_video_per_day_inc partition (dt = '$do_date')
select common.uid                                 user_id,
       appvideo.video_id,
       sum(appvideo.play_sec)                     total_time,
       max(cast(appvideo.position_sec as bigint)) max_time_position,
       '$do_date'                               first_time
from ods_log_inc
where dt = '$do_date'
  and appvideo.video_id is not null
group by common.uid, appvideo.video_id;
"
dwd_trade_cart_add_inc="
insert overwrite table dwd_trade_cart_add_inc partition(dt='$do_date')
select
    id,
    user_id,
    course_id,
    course_name,
    cart_price,
    session_id,
    date_format(create_time, 'yyyy-MM-dd') date_id,
    create_time,
    update_time,
    deleted,
    sold
from
(
    select
        data.id,
        data.user_id,
        if(old['deleted'] = 1,'null',data.course_id) course_id,
        data.course_name,
        data.cart_price,
        data.session_id,
        date_format(from_utc_timestamp(ts*1000,'GMT+8'),'yyyy-MM-dd') date_id,
        date_format(from_utc_timestamp(ts*1000,'GMT+8'),'yyyy-MM-dd HH:mm:ss') create_time,
        data.update_time,
        data.deleted,
        data.sold
    from ods_cart_info_inc
    where dt='$do_date'
    and type='insert'
)cart;
"
dwd_trade_cart_add_full=""
dwd_trade_cart_info_full="
insert overwrite table dwd_trade_cart_info_full partition (dt='$do_date')
select user_id,
       course_id,
       course_name,
       cart_price,
       create_time,
       update_time,
       sold
from ods_cart_info_full
where dt = '$do_date';
"
dwd_trade_order_detail_inc="
insert overwrite table dwd_trade_order_detail_inc partition (dt='$do_date')
select odd.order_id,
       odd.user_id,
       odd.course_id,
       od.province_id,
       od.order_status,
       odd.create_time,
       odd.order_date,
       odd.update_time,
       pay.pay_time,
       pay.payment_type,
       pay.payment_status,
       odd.origin_amount,
       odd.coupon_reduce,
       odd.final_amount
from (
     select data.order_id,
            data.user_id,
            data.course_id,
            data.origin_amount,
            data.coupon_reduce,
            data.final_amount,
            date_format(data.create_time,'yyyy-MM-dd') order_date,
            data.create_time,
            data.update_time
    from ods_order_detail_inc
    where dt = '$do_date'
    and type = 'insert'
 ) odd
left join (
    select data.id,
           data.province_id,
           data.order_status
    from ods_order_info_inc
    where dt = '$=do_date'
    and type = 'insert'
) od on odd.order_id = od.id
left join (
    select data.order_id,
           data.create_time,
           data.payment_type,
           data.payment_status,
           date_format(data.create_time,'yyyy-MM-dd') pay_time
    from ods_payment_info_inc
    where dt = '$do_date'
    and type = 'insert'
) pay on od.id = pay.order_id;
"
dwd_trade_pay_detail_suc_inc="
insert overwrite table dwd_trade_pay_detail_suc_inc partition (dt='$do_date')
select pay_success.id,
       pay_success.out_trade_no,
       pay_success.order_id,
       od.order_id,
       od.course_id,
       od.course_name,
       pay_success.trade_body,
       pay_success.payment_type,
       pay_success.alipay_trade_no,
       pay_success.total_amount,
       pay_success.create_time,
       pay_success.callback_time,
       pay_success.update_time
from (
     select data.id,
       data.order_id,
           data.payment_type,
       data.out_trade_no,
       data.trade_body,
       data.alipay_trade_no,
       data.total_amount,
            data.callback_time,
            data.update_time,
            data.create_time
from ods_payment_info_inc
where dt = '$do_date'
    and type = 'insert'
         )pay_success left join (
              select
            data.user_id,
            data.order_id,
            data.course_id,
            data.course_name,
            data.update_time
        from ods_order_detail_inc
        where dt = '$do_date'
             and type = 'insert'
    )od on pay_success.order_id = od.order_id;
"
dwd_traffic_page_view_inc="
insert overwrite table dwd_traffic_page_view_inc partition (dt='$do_date')
select
    province_id,
    brand,
    channel,
    is_new,
    model,
    mid_id,
    operate_system,
    user_id,
    version_code,
    page_item,
    page_item_type,
    last_page_id,
    page_id,
    source_id,
    source_site,
    source_url,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') view_time,
    concat(mid_id,'-',last_value(session_start_point,true) over (partition by mid_id order by ts)) session_id,
    during_time
from
(
    select
        concat(common.ar,'0000') area_code,
        common.ba brand,
        common.ch channel,
        common.is_new is_new,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code,
    	common.sc,
        page.during_time,
        page.item page_item,
        page.item_type page_item_type,
        page.last_page_id,
        page.page_id,
        ts,
        if(page.last_page_id is null,ts,null) session_start_point
    from ods_log_inc
    where dt='$do_date'
    and page is not null
)log
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code
left join
(
    select
        id source_id,
        source_site,
        source_url
    from ods_base_source_full
    where dt='$do_date'
) source on log.sc=source.source_id;
"
dwd_user_login_inc="
insert overwrite table dwd_user_login_inc partition(dt='$do_date')
select
    user_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd') date_id,
    date_format(from_utc_timestamp(ts,'GMT+8'),'yyyy-MM-dd HH:mm:ss') login_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        user_id,
        channel,
        area_code,
        version_code,
        mid_id,
        brand,
        model,
        operate_system,
        ts
    from
    (
        select
            user_id,
            channel,
            area_code,
            version_code,
            mid_id,
            brand,
            model,
            operate_system,
            ts,
            row_number() over (partition by session_id order by ts) rn
        from
        (
            select
                user_id,
                channel,
                area_code,
                version_code,
                mid_id,
                brand,
                model,
                operate_system,
                ts,
                concat(mid_id,'-',last_value(session_start_point,true) over(partition by mid_id order by ts)) session_id
            from
            (
                select
                    common.uid user_id,
                    common.ch channel,
                    concat(common.ar,'0000') area_code,
                    common.vc version_code,
                    common.mid mid_id,
                    common.ba brand,
                    common.md model,
                    common.os operate_system,
                    ts,
                    if(page.last_page_id is null,ts,null) session_start_point
                from ods_log_inc
                where dt='$do_date'
                and page is not null
            )t1
        )t2
        where user_id is not null
    )t3
    where rn=1
)t4
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='$do_date'
)bp
on t4.area_code=bp.area_code;
"
dwd_user_register_inc="
insert overwrite table dwd_user_register_inc partition(dt='$do_date')
select
    ui.user_id,
    date_format(create_time,'yyyy-MM-dd') date_id,
    create_time,
    channel,
    province_id,
    version_code,
    mid_id,
    brand,
    model,
    operate_system
from
(
    select
        data.id user_id,
        data.create_time
    from ods_user_info_inc
    where dt='$do_date'
    and type='insert'
)ui
left join
(
    select
        concat(common.ar,'0000') area_code,
        common.ba brand,
        common.ch channel,
        common.md model,
        common.mid mid_id,
        common.os operate_system,
        common.uid user_id,
        common.vc version_code
    from ods_log_inc
    where dt='$do_date'
    and common.uid is not null
)log
on ui.user_id=log.user_id
left join
(
    select
        id province_id,
        area_code
    from ods_base_province_full
    where dt='$do_date'
)bp
on log.area_code=bp.area_code;
"

case $1 in
"dwd_interaction_chapter_comment_inc")
	hive -e "$dwd_interaction_chapter_comment_inc"
;;
"dwd_interaction_course_comment_inc")
	hive -e "$dwd_interaction_course_comment_inc"
;;
"dwd_interaction_favor_add_inc")
	hive -e "$dwd_interaction_favor_add_inc"
;;
"dwd_study_user_chapter_process")
	hive -e "$dwd_study_user_chapter_process"
;;
"dwd_study_video_per_day_inc")
	hive -e "$dwd_study_video_per_day_inc"
;;
"dwd_trade_cart_add_inc")
	hive -e "$dwd_trade_cart_add_inc"
;;
"dwd_trade_cart_add_full")
	hive -e "$dwd_trade_cart_add_full"
;;
"dwd_trade_cart_info_full")
	hive -e "$dwd_trade_cart_info_full"
;;
"dwd_trade_order_detail_inc")
	hive -e "$dwd_trade_order_detail_inc"
;;
"dwd_trade_pay_detail_suc_inc")
	hive -e "$dwd_trade_pay_detail_suc_inc"
;;
"dwd_traffic_page_view_inc")
	hive -e "$dwd_traffic_page_view_inc"
;;
"dwd_user_login_inc")
	hive -e "$dwd_user_login_inc"
;;
"dwd_user_register_inc")
	hive -e "$dwd_user_register_inc"
;;
"all")
	hive -e "$dwd_interaction_chapter_comment_inc$dwd_interaction_course_comment_inc$dwd_interaction_favor_add_inc$dwd_study_user_chapter_process$dwd_study_video_per_day_inc$dwd_trade_cart_add_inc$dwd_trade_cart_add_full$dwd_trade_cart_info_full$dwd_trade_order_detail_inc$dwd_trade_pay_detail_suc_inc$dwd_traffic_page_view_inc$dwd_user_login_inc$dwd_user_register_inc"
;;
esac
