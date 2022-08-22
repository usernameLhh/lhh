#!/bin/bash

APP=edu

if [ -n "$2" ] ;then
   do_date=$2
else 
   do_date=`date -d '-1 day' +%F`
fi

load_data(){
    sql=""
    for i in $*; do
        #判断路径是否存在
        hadoop fs -test -e /origin_data/$APP/db/${i:4}/$do_date
        #路径存在方可装载数据
        if [[ $? = 0 ]]; then
            sql=$sql"load data inpath '/origin_data/$APP/db/${i:4}/$do_date' OVERWRITE into table ${APP}.$i partition(dt='$do_date');"
        fi
    done
    hive -e "$sql"
}

case $1 in
    "ods_base_category_info_full")
        load_data "ods_base_category_info_full"
    ;;
    "ods_base_province_full")
        load_data "ods_base_province_full"
    ;;
    "ods_base_source_full")
        load_data "ods_base_source_full"
    ;;
    "ods_base_subject_info_full")
        load_data "ods_base_subject_info_full"
    ;;
    "ods_cart_info_full")
        load_data "ods_cart_info_full"
    ;;
    "ods_chapter_info_full")
        load_data "ods_chapter_info_full"
    ;;
    "ods_course_info_full")
        load_data "ods_course_info_full"
    ;;
    "ods_knowledge_point_full")
        load_data "ods_knowledge_point_full"
    ;;
    "ods_test_paper_full")
        load_data "ods_test_paper_full"
    ;;
    "ods_test_question_info_full")
        load_data "ods_test_question_info_full"
    ;;
    "ods_video_info_full")
        load_data "ods_video_info_full"
    ;;
    "ods_cart_info_inc")
        load_data "ods_cart_info_inc"
    ;;
    "ods_comment_info_inc")
        load_data "ods_comment_info_inc"
    ;;
    "ods_favor_info_inc")
        load_data "ods_favor_info_inc"
    ;;
    "ods_order_detail_inc")
        load_data "ods_order_detail_inc"
    ;;

    "ods_order_info_inc")
        load_data "ods_order_info_inc"
    ;;
    "ods_payment_info_inc")
        load_data "ods_payment_info_inc"
    ;;
    "ods_review_info_inc")
        load_data "ods_review_info_inc"
    ;;
    "ods_test_exam_question_inc")
        load_data "ods_test_exam_question_inc"
    ;;
    "ods_test_paper_question_inc")
        load_data "ods_test_paper_question_inc"
    ;;
    "ods_test_point_question_inc")
        load_data "ods_test_point_question_inc"
    ;;
    "ods_test_question_option_inc")
        load_data "ods_test_question_option_inc"
    ;;
    "ods_user_chapter_process_inc")
        load_data "ods_user_chapter_process_inc"
    ;;
    "ods_user_info_inc")
        load_data "ods_user_info_inc"
    ;;
    "ods_vip_change_detail_inc")
        load_data "ods_vip_change_detail_inc"
    ;;
    "ods_test_exam_inc")
        load_data "ods_test_exam_inc"
    ;;
    "all")
        load_data "ods_base_category_info_full" "ods_base_province_full" "ods_base_source_full" "ods_base_subject_info_full" "ods_cart_info_full" "ods_chapter_info_full" "ods_course_info_full" "ods_knowledge_point_full" "ods_test_paper_full" "ods_test_question_info_full" "ods_video_info_full" "ods_cart_info_inc" "ods_comment_info_inc" "ods_favor_info_inc" "ods_order_detail_inc" "ods_order_info_inc" "ods_payment_info_inc" "ods_review_info_inc" "ods_test_exam_question_inc" "ods_test_paper_question_inc" "ods_test_point_question_inc" "ods_test_question_option_inc" "ods_user_chapter_process_inc" "ods_user_info_inc" "ods_vip_change_detail_inc" "ods_test_exam_inc"
    ;;
esac

