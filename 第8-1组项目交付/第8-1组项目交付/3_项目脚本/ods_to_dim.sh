#!/bin/bash

if [ -n "$2" ] ;then
	do_date=$2
else
	do_date=`date -d "-1 day" + %F`
fi

dim_course_full="
use edu;
insert overwrite table edu.dim_course_full partition(dt='$do_date')
select
     course.course_id    course_id 	 
     , course_name                     
     , course.subject_id   subject_id 
     , subject_name                    
     , subject.category_id category_id
     , category_name                   
     , chapter_num                    
     , chapter_id                     
     , chapter_name                  
     , is_free                        
     , teacher                        
     , origin_price                    
     , reduce_amount                   
     , actual_price                    
     , create_time                   
     , update_time                   
     , deleted                        
from
(
    select
        id course_id,
        course_name,
        subject_id,
        teacher,
        chapter_num,
        origin_price,
        reduce_amount,
        actual_price,
        create_time,
        update_time,
        deleted
    from ods_course_info_full
    where dt = '$do_date'
) course
left join
(
    select id subject_id,
           subject_name,
           category_id
    from ods_base_subject_info_full
    where dt = '$do_date'
) subject on subject.subject_id = course.subject_id
left join
(
    select id category_id,
           category_name
    from ods_base_category_info_full
    where dt = '$do_date'
) category on category.category_id = subject.category_id
left join
(
    select
       id chapter_id,
       chapter_name,
       is_free,
       course_id
    from ods_chapter_info_full
    where dt = '$do_date'
) chapter on chapter.course_id = course.course_id;
"

dim_exam_full="
insert overwrite table edu.dim_exam_full partition(dt='$do_date')
select                       
    exam_id              
    ,paper_title           
    ,user_id              
    ,exam.paper_id paper_id
    ,course_id            
    ,score               
    ,duration_sec     
    ,create_time          
    ,submit_time          
    ,update_time          
    ,deleted              
    ,exam_question_id      
    ,exam_question_answer  
    ,exam_question_is_correct
    ,exam_question_score
from (
    select
        data.id,
        data.paper_id,
        data.user_id,
        data.score,
        data.duration_sec,
        data.create_time,
        data.submit_time,
        data.update_time,
        data.deleted
    from ods_test_exam_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) exam
left join (
    select
       data.exam_id,
       data.paper_id,
       data.question_id exam_question_id,
       data.answer exam_question_answer,
       data.is_correct exam_question_is_correct,
       data.score exam_question_score
    from ods_test_exam_question_inc
    where dt = '$do_date'
    and type = 'bootstrap-insert'
) exam_question on exam.id = exam_question.exam_id
left join (
    select id,
           paper_title,
           course_id
    from ods_test_paper_full
    where dt = '$do_date'
) paper on paper.id = exam.paper_id;
"
dim_paper_full="
with paper as (
    select id,
           paper_title,
           course_id,
           create_time,
           update_time,
           publisher_id,
           deleted,
           dt
    from ods_test_paper_full
    where dt = '$do_date'
),
     course as (
         select id,
                course_name,
                subject_id
         from ods_course_info_full
         where dt = '$do_date'
     ),
     sub as (
         select id,
                subject_name,
                category_id
         from ods_base_subject_info_full
         where dt = '$do_date'
     ),
     cate as (
         select id,
                category_name
         from ods_base_category_info_full
            where dt = '$do_date'
     ),
     question as (
         select id,
                chapter_id,
                question_type,
                question_txt,
                course_id
         from ods_test_question_info_full
            where dt = '$do_date'
     ),q_option as (
         select data.question_id,
                data.is_correct
         from ods_test_question_option_inc
         where dt = '$do_date'
)
insert overwrite table dim_paper_full partition (dt='$do_date')
select paper.id,
       paper_title,
       paper.course_id,
       course_name,
       course.subject_id,
       sub.subject_name,
       sub.category_id,
       cate.category_name,
       question.id,
       question.question_type,
        question_txt,
       is_correct,
       create_time,
       update_time,
       publisher_id,
       deleted
from paper
         left join course on paper.course_id = course.id
         left join sub on course.subject_id = sub.id
         left join cate on sub.category_id = cate.id
         left join question on paper.course_id = question.course_id
left join q_option on question.id = q_option.question_id;"
dim_province_full="
use edu;
insert overwrite table dim_province_full partition (dt='$do_date')
select id,
       name,
       area_code,
       iso_code,
       iso_3166_2,
       region_id
from ods_base_province_full
where dt = '$do_date';"
dim_user_zip="
insert overwrite table edu.dim_user_zip partition(dt)
select
    id,
    login_name,
    nick_name,
    passwd,
    real_name,
    phone_num,
    email,
    head_img,
    user_level,
    birthday,
    gender,
    create_time,
    operate_time,
    status,
    start_date,
    if(rn=2,date_sub('$do_date',1),end_date) end_date,
    if(rn=1,'9999-12-31',date_sub('$do_date',1)) dt
from
(
    select
        id,
        login_name,
        nick_name,
        passwd,
        real_name,
        phone_num,
        email,
        head_img,
        user_level,
        birthday,
        gender,
        create_time,
        operate_time,
        status,
        start_date,
        end_date,
        row_number() over (partition by id order by start_date desc) rn
    from
    (
        select
            id,
            login_name,
            nick_name,
            passwd,
            real_name,
            phone_num,
            email,
            head_img,
            user_level,
            birthday,
            gender,
            create_time,
            operate_time,
            status,
            start_date,
            end_date
        from dim_user_zip
        where dt='9999-12-31'
        union
        select
           id,
           login_name,
           nick_name,
           passwd,
           real_name,
           phone_num,
           email,
           head_img,
           user_level,
           birthday,
           gender,
           create_time,
           operate_time,
           status,
           '$do_date' start_date,
           '9999-12-31' end_date
        from
        (
            select
               data.id,
               data.login_name,
               data.nick_name,
               data.passwd,
               data.real_name,
               data.phone_num,
               data.email,
               data.head_img,
               data.user_level,
               data.birthday,
               data.gender,
               data.create_time,
               data.operate_time,
               data.status,
               row_number() over (partition by data.id order by ts desc) rn
            from ods_user_info_inc
            where dt='$do_date'
        )t1
        where rn=1
    )t2
)t3;
"
dim_video_full="
insert overwrite table dim_video_full partition (dt = '$do_date')
select v.id,
       video_name,
       during_sec video_during_sec,
       video_size,
       video_source_id,
       v.chapter_id,
       c.chapter_name,
       v.course_id,
       cs.course_name,
       cs.subject_id,
       s.subject_name,
       s.category_id,
       category_name,
       teacher,
       v.create_time,
       v.update_time
from (select id,
             video_name,
             during_sec,
             video_size,
             video_source_id,
             chapter_id,
             course_id,
             create_time,
             update_time
      from ods_video_info_full
      where dt = '$do_date') v
         left join (select id, chapter_name from ods_chapter_info_full where dt = '$do_date') c on v.chapter_id = c.id
         left join (select id, course_name, subject_id, teacher from ods_course_info_full where dt = '$do_date') cs
                   on v.course_id = cs.id
         left join (select id, subject_name, category_id from ods_base_subject_info_full where dt = '$do_date') s
                   on cs.subject_id = s.id
         left join (select id, category_name from ods_base_category_info_full where dt = '$do_date') ct;
"

case $1 in
"dim_course_full")
	hive -e "$dim_course_full"
;;
"dim_exam_full")
	hive -e "$dim_exam_full"
;;
"dim_paper_full")
	hive -e "$dim_paper_full"
;;
"dim_province_full")
	hive -e "$dim_province_full"
;;
"dim_user_zip")
	hive -e "$dim_user_zip"
;;
"dim_video_full")
	hive -e "$dim_video_full"
;;
"all")
	hive -e "$dim_course_full$dim_exam_full$dim_paper_full$dim_province_full$dim_user_zip$dim_video_full"
;;
esac
