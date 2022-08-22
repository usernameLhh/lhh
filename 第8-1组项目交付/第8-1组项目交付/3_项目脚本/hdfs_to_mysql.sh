#! /bin/bash

DATAX_HOME=/opt/module/datax

#DataX导出路径不允许存在空文件，该函数作用为清理空文件
handle_export_path(){
  for i in `hadoop fs -ls -R $1 | awk '{print $8}'`; do
    hadoop fs -test -z $i
    if [[ $? -eq 0 ]]; then
      echo "$i文件大小为0，正在删除"
      hadoop fs -rm -r -f $i
    fi
  done
}

#数据导出
export_data() {
  datax_config=$1
  export_dir=$2
  handle_export_path $export_dir
  $DATAX_HOME/bin/datax.py -p"-Dexportdir=$export_dir" $datax_config
}

case $1 in
  "ads_exam_by_paper")
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_paper.json /warehouse/edu/ads/ads_exam_by_paper
  ;;
  "ads_exam_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_course.json /warehouse/edu/ads/ads_exam_by_course
  ;;  
  "ads_exam_by_papar_score")
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_papar_score.json /warehouse/edu/ads/ads_exam_by_papar_score
  ;;
  "ads_exam_by_question")
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_question.json /warehouse/edu/ads/ads_exam_by_question
  ;;
  "ads_user_change")
    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change
  ;;
  "ads_user_retention")
    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention
  ;;  
  "ads_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats
  ;;
  "ads_user_action")
    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action
  ;;
  "ads_new_order_user_stats")
    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats
  ;;
  "ads_trade_by_old")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_by_old.json /warehouse/edu/ads/ads_trade_by_old
  ;;  
  "ads_traffic_stats_by_channel")
    export_data /opt/module/datax/job/export/edu_report.ads_traffic_stats_by_channel.json /warehouse/edu/ads/ads_traffic_stats_by_channel
  ;;
  "ads_page_path")
    export_data /opt/module/datax/job/export/edu_report.ads_page_path.json /warehouse/edu/ads/ads_page_path
  ;;
  "ads_course_order_by_cate")
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_cate.json /warehouse/edu/ads/ads_course_order_by_cate
  ;;
  "ads_course_order_by_subject")
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_subject.json /warehouse/edu/ads/ads_course_order_by_subject
  ;;  
  "ads_course_order_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_course.json /warehouse/edu/ads/ads_course_order_by_course
  ;;
  "ads_review_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_review_by_course.json /warehouse/edu/ads/ads_review_by_course
  ;;
  "ads_study_finish_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_study_finish_by_course.json /warehouse/edu/ads/ads_study_finish_by_course
  ;;
  "ads_study_finish_avg_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_study_finish_avg_by_course.json /warehouse/edu/ads/ads_study_finish_avg_by_course
  ;;
  "ads_study_finish_KPI")
    export_data /opt/module/datax/job/export/edu_report.ads_study_finish_KPI.json /warehouse/edu/ads/ads_study_finish_KPI
  ;;
  "ads_study_video_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_study_video_by_course.json /warehouse/edu/ads/ads_study_video_by_course
  ;;
  "ads_study_video_by_chapter")
    export_data /opt/module/datax/job/export/edu_report.ads_study_video_by_chapter.json /warehouse/edu/ads/ads_study_video_by_chapter
  ;;
  "ads_trade_order_by_days")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_order_by_days.json /warehouse/edu/ads/ads_trade_order_by_days
  ;;
  "ads_trade_order_by_province")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_order_by_province.json /warehouse/edu/ads/ads_trade_order_by_province
  ;;
  "ads_trade_video_retention_by_category")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_category.json /warehouse/edu/ads/ads_trade_video_retention_by_category
  ;;
  "ads_trade_video_retention_by_subject")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_subject.json /warehouse/edu/ads/ads_trade_video_retention_by_subject
  ;;
  "ads_trade_video_retention_by_course")
    export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_course.json /warehouse/edu/ads/ads_trade_video_retention_by_course
  ;;
  

  
  "all")
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_paper.json /warehouse/edu/ads/ads_exam_by_paper
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_course.json /warehouse/edu/ads/ads_exam_by_course
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_papar_score.json /warehouse/edu/ads/ads_exam_by_papar_score
    export_data /opt/module/datax/job/export/edu_report.ads_exam_by_question.json /warehouse/edu/ads/ads_exam_by_question
    export_data /opt/module/datax/job/export/edu_report.ads_user_change.json /warehouse/edu/ads/ads_user_change
    export_data /opt/module/datax/job/export/edu_report.ads_user_retention.json /warehouse/edu/ads/ads_user_retention
    export_data /opt/module/datax/job/export/edu_report.ads_user_stats.json /warehouse/edu/ads/ads_user_stats
    export_data /opt/module/datax/job/export/edu_report.ads_user_action.json /warehouse/edu/ads/ads_user_action
    export_data /opt/module/datax/job/export/edu_report.ads_new_order_user_stats.json /warehouse/edu/ads/ads_new_order_user_stats
    export_data /opt/module/datax/job/export/edu_report.ads_trade_by_old.json /warehouse/edu/ads/ads_trade_by_old
    export_data /opt/module/datax/job/export/edu_report.ads_traffic_stats_by_channel.json /warehouse/edu/ads/ads_traffic_stats_by_channel
    export_data /opt/module/datax/job/export/edu_report.ads_page_path.json /warehouse/edu/ads/ads_page_path
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_cate.json /warehouse/edu/ads/ads_course_order_by_cate
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_subject.json /warehouse/edu/ads/ads_course_order_by_subject
    export_data /opt/module/datax/job/export/edu_report.ads_course_order_by_course.json /warehouse/edu/ads/ads_course_order_by_course
    export_data /opt/module/datax/job/export/edu_report.ads_review_by_course.json /warehouse/edu/ads/ads_review_by_course
	export_data /opt/module/datax/job/export/edu_report.ads_study_finish_by_course.json /warehouse/edu/ads/ads_study_finish_by_course
	export_data /opt/module/datax/job/export/edu_report.ads_study_finish_avg_by_course.json /warehouse/edu/ads/ads_study_finish_avg_by_course
	export_data /opt/module/datax/job/export/edu_report.ads_study_finish_KPI.json /warehouse/edu/ads/ads_study_finish_KPI
	export_data /opt/module/datax/job/export/edu_report.ads_study_video_by_course.json /warehouse/edu/ads/ads_study_video_by_course
	export_data /opt/module/datax/job/export/edu_report.ads_study_video_by_chapter.json /warehouse/edu/ads/ads_study_video_by_chapter
	export_data /opt/module/datax/job/export/edu_report.ads_trade_order_by_days.json /warehouse/edu/ads/ads_trade_order_by_days
	export_data /opt/module/datax/job/export/edu_report.ads_trade_order_by_province.json /warehouse/edu/ads/ads_trade_order_by_province
	export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_category.json /warehouse/edu/ads/ads_trade_video_retention_by_category
	export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_subject.json /warehouse/edu/ads/ads_trade_video_retention_by_subject
	export_data /opt/module/datax/job/export/edu_report.ads_trade_video_retention_by_course.json /warehouse/edu/ads/ads_trade_video_retention_by_course
  ;;
esac
