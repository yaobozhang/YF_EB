CREATE PROCEDURE DM.SP_MEMBER_CNT_INFO_V3(IN endTime NVARCHAR(8),MONTH_QTY int)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
-------------修改历史-----------------------------------------
--时间:20190814
--版本:会员标签版3
--开发人员:聂鑫、薛艳、宁颖琦
--目的：新增会员标签，优化旧标签字段
-------------修改历史-----------------------------------------
--20190821   薛艳      新增日志
--20190905   薛艳      切换成正式的会员标签表
--20191023	 宁颖琦	   删除部分字段 在（var_base 与var_main_comsp 中部分或全部字段）

--计算各个视图的起始入参
var_offline_begin_time nvarchar(8) :=to_char( add_days(last_day(add_months(to_date(:endTime, 'yyyymmdd'), -:MONTH_QTY)),1),'yyyymmdd') ;
var_memb_static_time nvarchar(8) := to_char(add_days(add_years(to_date(:endTime,'yyyymmdd'), -1),1),'yyyymmdd');
var_main_info_time nvarchar(8) := to_char(add_days(add_months(to_date(:endTime,'yyyymmdd'),-6),1),'yyyymmdd');


-------------------------------------日志参数初始化begin--------------------------
var_sp_name varchar(50) :='DM.SP_MEMBER_CNT_INFO_V3';--sp名称
var_stsc_date nvarchar(20) :=to_char(current_date,'yyyymmdd') ;--跑数日期(频率到天)
var_log_begin_time LONGDATE ;   				--开始时间
var_log_end_time LONGDATE;                      --SQL结束时间
var_sql_step int ;                          	--SQL步骤
var_step_comments varchar(500);             	--步骤说明
-------------------------------------日志参数初始化end----------------------------

begin
--step1:获取会员基础信息

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=1;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取会员基础字段信息';                --步骤说明

-------------------------------------日志参数end----------------------------------
var_base =                     --t1
SELECT
	 "MEMBER_ID",
	 "GNDR",
	 "BIRT_DATE",
	 "BELONE_PVNC_NAME",
	 "BELONE_CITY_NAME",
	 "BELONE_COUNTY_NAME",
	 "MEMB_CARD_STATE",
	 "MEMB_SOUR",
	 "IS_WECHAT",
	 "IS_ALIPAY",
	 "IS_YF",
	 "OPEN_CARD_TIME",
	 "OPEN_CARD_EMPE",
	 "OPEN_CARD_PHMC_CODE",
	 "OPEN_CARD_PHMC_NAME",
	 "OPEN_CARD_PHMC_STAR_BUSI",
	 "OPEN_CARD_PHMC_IS_MEDI_INSU",
	 "OPEN_CARD_PHMC_PROR_ATTR",
	 "OPEN_CARD_COMPANY_CODE",
	 "OPEN_CARD_COMPANY_NAME",
	 "OPEN_CARD_DEPRT_CODE",
	 "OPEN_CARD_DEPRT_NAME",
	 "OPEN_CARD_DIST_CODE",
	 "OPEN_CARD_DIST_NAME",
	 "GNDR_AGE_TYPE" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_LABEL_BASE_INFO" --修改by 宁颖琦 20191023
/*	 注释by 宁颖琦 20191023
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_LABEL_BASE_INFO"(
PLACEHOLDER."$$BeginTime$$" => :var_offline_begin_time,
PLACEHOLDER."$$EndTime$$" => :endTime)
*/
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 


--step2:获取会员总体销售数据

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=2;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取总销售字段信息';                --步骤说明

-------------------------------------日志参数end----------------------------------

var_total_comsp =               --t2
select
	 "STSC_DATE",
	 "MEMBER_ID",
	 "MEMBER_TYPE",
	 max("IS_ABNO_CARD") AS "IS_ABNO_CARD",
	 max("IS_NEGA_CARD") AS "IS_NEGA_CARD",
	 max("IS_EASY_MARKETING") AS "IS_EASY_MARKETING",
	 max("IS_ACTIV_MEMB") AS "IS_ACTIV_MEMB",
	 sum("UNIT_PRI") AS "UNIT_PRI",
	  max("UNIT_PRI_TYPE") AS "UNIT_PRI_TYPE",
	 max("GROS_MARGIN_TYPE") AS "GROS_MARGIN_TYPE", 
	 sum("R_ALL_SALE_AMT") AS "R_ALL_SALE_AMT",
	 sum("R_ALL_GROSS_AMT") AS "R_ALL_GROSS_AMT",
	 sum("R_ALL_SONSU_TIMES") AS "R_ALL_SONSU_TIMES",
	 sum("CNT_R_ALL_SALE_AMT") AS "CNT_R_ALL_SALE_AMT",
	 sum("CNT_R_ALL_GROSS_AMT") AS "CNT_R_ALL_GROSS_AMT",
	 sum("CNT_R_ALL_SONSU_TIMES") AS "CNT_R_ALL_SONSU_TIMES",
	 sum("CNT_R_ALL_NCD_CNT") AS "CNT_R_ALL_NCD_CNT",
	 sum("CNT_OFFLINE_TOTAL_CNSM_AMT") AS "CNT_OFFLINE_TOTAL_CNSM_AMT",
	 sum("CNT_OFFLINE_TOTAL_GROS_AMT") AS "CNT_OFFLINE_TOTAL_GROS_AMT",
	 sum("CNT_OFFLINE_TOTAL_CNSM_TIMES") AS "CNT_OFFLINE_TOTAL_CNSM_TIMES",
	 sum("GROS_MARGIN") AS "GROS_MARGIN",
	 sum("M_SALE_AMT") AS "M_SALE_AMT",
	 sum("M_GROSS_AMT") AS "M_GROSS_AMT",
	 sum("M_SONSU_TIMES") AS "M_SONSU_TIMES",
	 sum("L_M_SALE_AMT") AS "L_M_SALE_AMT",
	 sum("L_M_GROSS_AMT") AS "L_M_GROSS_AMT",
	 sum("L_M_SONSU_TIMES") AS "L_M_SONSU_TIMES",
	 sum("Q_SALE_AMT") AS "Q_SALE_AMT",
	 sum("Q_GROSS_AMT") AS "Q_GROSS_AMT",
	 sum("Q_SONSU_TIMES") AS "Q_SONSU_TIMES",
	 sum("R_HALF_SALE_AMT") AS "R_HALF_SALE_AMT",
	 sum("R_HALF_GROSS_AMT") AS "R_HALF_GROSS_AMT",
	 sum("R_HALF_SONSU_TIMES") AS "R_HALF_SONSU_TIMES",
	 sum("R_YEAR_SALE_AMT") AS "R_YEAR_SALE_AMT",
	 sum("R_YEAR_GROSS_AMT") AS "R_YEAR_GROSS_AMT",
	 sum("R_YEAR_SONSU_TIMES") AS "R_YEAR_SONSU_TIMES",
	 sum("R_LAST_YEAR_SALE_AMT") AS "R_LAST_YEAR_SALE_AMT",
	 sum("R_LAST_YEAR_GROSS_AMT") AS "R_LAST_YEAR_GROSS_AMT",
	 sum("R_LAST_YEAR_SONSU_TIMES") AS "R_LAST_YEAR_SONSU_TIMES",
	 sum("R_YEAR_YX_SALE_AMT") AS "R_YEAR_YX_SALE_AMT",
	 sum("R_YEAR_YX_GROSS_AMT") AS "R_YEAR_YX_GROSS_AMT",
	 sum("MID_SALE_AMT") AS "MID_SALE_AMT",
	 sum("MID_GROSS_AMT") AS "MID_GROSS_AMT",
	 sum("MID_SONSU_TIMES") AS "MID_SONSU_TIMES",
	 sum("FF_SONSU_TIMES") AS "FF_SONSU_TIMES",
	 sum("TWENTY_SONSU_TIMES") AS "TWENTY_SONSU_TIMES",
	 sum("TF_SONSU_TIMES") AS "TF_SONSU_TIMES",
	 sum("FOF_SONSU_TIMES") AS "FOF_SONSU_TIMES" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_TOTAL_SALE_INFO"(PLACEHOLDER."$$EndTime$$" => :endTime,
                                                       PLACEHOLDER."$$MONTH_QTY$$" => :MONTH_QTY) 
GROUP BY 	 
	 "STSC_DATE",
	 "MEMBER_ID",
	 "MEMBER_TYPE"
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 



--step3:获取会员首次+最后消费日期;最大+最小+平均购买间距;线下首次+最后消费日期、门店、金额

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=3;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取线下字段信息';                --步骤说明

-------------------------------------日志参数end----------------------------------

var_line_comsp =                      --t3
SELECT
	 "MEMBER_ID",
	 "FST_CUNSU_DATE",
	 "LAST_TIME_CUNSU_DATE",
	 "OFFLINE_FST_CNSM_DATE",
	 "OFFLINE_FST_CNSM_PHMC_CODE",
     "OFFLINE_FST_CNSM_PHMC_NAME",	 
	 "OFFLINE_LAST_CNSM_DATE",
	 "OFFLINE_LAST_CNSM_PHMC_CODE",
	 "OFFLINE_LAST_CNSM_PHMC_NAME",
	 "NO_CONSU_TIME_LONG_MEMB_BEHAV",
	 sum("OFFLINE_FST_CNSM_AMT") AS "OFFLINE_FST_CNSM_AMT",
	 sum("OFFLINE_LAST_CNSM_AMT") AS "OFFLINE_LAST_CNSM_AMT",
	 sum("R_MAX_SALE_INTERVAL") AS "R_MAX_SALE_INTERVAL",
	 sum("R_MIN_SALE_INTERVAL") AS "R_MIN_SALE_INTERVAL"
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_OFF_LINE_TOTAL_COMSUMP"(
	 PLACEHOLDER."$$BeginTime$$" => :var_offline_begin_time,
	 PLACEHOLDER."$$EndTime$$" => :endTime,
	 PLACEHOLDER."$$MONTH_QTY$$" => :MONTH_QTY)
GROUP BY "MEMBER_ID",
	 "FST_CUNSU_DATE",
	 "LAST_TIME_CUNSU_DATE",
	 "OFFLINE_FST_CNSM_DATE",
	 "OFFLINE_FST_CNSM_PHMC_CODE",
	 "OFFLINE_LAST_CNSM_DATE",
	 "OFFLINE_LAST_CNSM_PHMC_CODE",
	 "OFFLINE_LAST_CNSM_PHMC_NAME",
	 "OFFLINE_FST_CNSM_PHMC_NAME",
	 "NO_CONSU_TIME_LONG_MEMB_BEHAV"
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 


--step4:获取会员支付方式各标签

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=4;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取统计各标签';                --步骤说明

-------------------------------------日志参数end----------------------------------
var_goods_payMode =                  --t4
SELECT
	 "MEMBER_ID",
	 "NCD_TYPE",
	 max("R_YEAR_TIME_PREFER") AS "R_YEAR_TIME_PREFER",
	 max("IS_WX_PAY_MEMB_BEHAV") AS "IS_WX_PAY_MEMB_BEHAV",
	 max("IS_ALI_PAY_MEMB_BEHAV") AS "IS_ALI_PAY_MEMB_BEHAV",
	 max("IS_BANKCARD_PAY_MEMB_BEHAV") AS "IS_BANKCARD_PAY_MEMB_BEHAV",
	 max("R_YEAR_IS_MEDI") AS "R_YEAR_IS_MEDI",
	 sum("NCD_CNT") AS "NCD_CNT",
	 sum("M_NCD_NCT") AS "M_NCD_NCT",
	 max("R_YEAR_CNSM_GOODS_CODE") AS "R_YEAR_CNSM_GOODS_CODE",
	 max("R_YEAR_CNSM_GOODS_NAME") AS "R_YEAR_CNSM_GOODS_NAME",
	 max("R_YEAR_CNSM_CATE_CODE") AS "R_YEAR_CNSM_CATE_CODE",
	 max("R_YEAR_CNSM_CATE_NAME") AS "R_YEAR_CNSM_CATE_NAME" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_STATIC_INFO"(
	 PLACEHOLDER."$$BeginTime$$" => :var_memb_static_time ,
	 PLACEHOLDER."$$EndTime$$" => :endTime,
     PLACEHOLDER."$$MONTH_QTY$$" => :MONTH_QTY)
GROUP BY "MEMBER_ID","NCD_TYPE"
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 



--step5:获取会员主消费公司片区门管门店

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=5;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取会员主消费公司片区门管门店';                --步骤说明

-------------------------------------日志参数end----------------------------------
/* 注释by 宁颖琦 20191023
var_main_comsp =                --t6
SELECT
	 "MEMBER_ID",
	 "MAIN_CNSM_PHMC_CODE",
	 "MAIN_CNSM_PHMC_NAME",
	 "MAIN_CNSM_COMPANY_CODE",
	 "MAIN_CNSM_COMPANY_NAME",
	 "MAIN_CNSM_DEPRT_CODE",
	 "MAIN_CNSM_DEPRT_NAME",
	 "MAIN_CNSM_DIST_CODE",
	 "MAIN_CNSM_DIST_NAME" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_MAIN_INFO"	 (
	 PLACEHOLDER."$$Begin_Time$$" => :var_main_info_time,
	 PLACEHOLDER."$$End_Time$$" => :endTime)
 
;
*/
-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 


--step6:获取会员血糖标签


-------------------------------------日志参数begin--------------------------------
var_sql_step  :=6;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取会员血糖标签';                --步骤说明

-------------------------------------日志参数end----------------------------------
var_bp_bs=                          --t8
 SELECT
	 "CUSTOMER_ID",
	 max("LAST_BP_TIME") AS "LAST_BP_TIME",
	 max("LAST_BS_TIME") AS "LAST_BS_TIME" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_NCD_INFO"(PLACEHOLDER."$$End_Time$$" => :endTime)
GROUP BY "CUSTOMER_ID"
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 

--step7:获取会员线下各标签

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=7;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='获取会员线下各销售标签';                --步骤说明

-------------------------------------日志参数end----------------------------------
var_agg=                            --t9
SELECT
	 "MEMBER_ID",
     sum("Q_SALE_AMT_STSC") AS "OFFLINE_Q_CNSM_AMT",	 
     sum("M_GROSS_AMT_STSC")as "OFFLINE_Q_GROSS_AMT",
     sum("Q_SONSU_TIMES_STSC") AS "OFFLINE_Q_CNSM_TIMES",
     sum("R_YEAR_SALE_AMT_STSC") AS "OFFLINE_Y_CNSM_AMT",
	 sum("R_YEAR_GROSS_AMT_STSC") AS "OFFLINE_Y_GROSS_AMT",
	 sum("R_YEAR_SONSU_TIMES_STSC") AS "OFFLINE_Y_CNSM_TIMES",
	 sum("MID_SALE_AMT_STSC") AS "MID_SALE_AMT_STSC",
	 sum("MID_GROSS_AMT_STSC") AS "MID_GROSS_AMT_STSC",
	 sum("MID_SONSU_TIMES_STSC") AS "MID_SONSU_TIMES_STSC"	 
FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_MEMBER_SALE_AGG_INFO_V3"(PLACEHOLDER."$$endTime$$" => :endTime,
     PLACEHOLDER."$$MONTH_QTY$$" => :MONTH_QTY)
where "SOURCE" = 'off-line'
GROUP BY "MEMBER_ID"
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 

--step8:得到最终结果

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=8;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='汇总最终结果';                --步骤说明

-------------------------------------日志参数end----------------------------------
var_total=

select :endTime AS DATA_DATE,        --数据日期
	 t1.MEMBER_ID,                 --会员ID
     t1.IS_ACTIV_MEMB,	           --是否活跃会员 
	 t1.IS_ABNO_CARD,              --是否异常卡
	 t1.IS_NEGA_CARD  ,        --是否负卡
	 t1.IS_EASY_MARKETING,     --是否易营销 
     NULL AS LIFE_CYCLE_TYPE,      --时间频次生命周期类型	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --新客无消费
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --新客有消费
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --成长期
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --成熟期
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --衰退期
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --中度沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --深度沉睡
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --完整生命周期类型 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --客户标识(0302表新客无消费)
     t1.GNDR_AGE_TYPE,            --性别年龄类型
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --客单价
     t2.UNIT_PRI_TYPE,            --客单价类型
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --毛利率
     t2.GROS_MARGIN_TYPE,         --毛利率类型
     t3.LAST_TIME_CUNSU_DATE,     --最后一次消费时间
	 t3.R_MAX_SALE_INTERVAL,       --统计最大消费间距
     t3.R_MIN_SALE_INTERVAL,       --统计最小消费间距
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --平均消费间隔间距
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --未消费时间长
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --是否支付宝支付
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --是否微信支付
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --是否刷卡支付
	 --t5.values_R_1 as CLNT_CALU,          --客户价值   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --历史客户价值  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'本月销售额
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'本月毛利额
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'本月消费次数
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'上月销售额
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'上月毛利额
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'上月消费次数
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'最近三个月销售额
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'最近三个月毛利额
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'最近三个月消费次数
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'近半年销售额
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'近半年毛利额
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'近半年消费次数
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'近一年销售额
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'近一年毛利额
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'近一年消费次数
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'上一年销售额
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'上一年毛利额
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'上一年消费次数
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'累计销售额
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'累计毛利额
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'累计消费次数
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --慢病会员标识
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --近一年慢病品类购买次数
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --累计慢病品类购买次数
     --t5.value_level as CLNT_CALU_LEVEL,--客户价值类型 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --首次消费时间
     t1.GNDR,                          --'性别
     t1.BIRT_DATE,                     --'出生日期
     t1.BELONE_PVNC_NAME,              --'所属省份
     t1.BELONE_CITY_NAME,              --'所属市
     t1.BELONE_COUNTY_NAME,            --'所属区县
     t1.MEMB_CARD_STATE,               --'会员卡状态
     t1.MEMB_SOUR,                     --'会员来源
     t1.IS_WECHAT,                     --'是否绑定微信会员
     t1.IS_ALIPAY,                     --'是否绑定支付宝会员
     t1.IS_YF,                         --'是否关注益丰大药房
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --未消费顾客
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --低活跃度
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --忠诚
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --高活跃度
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --低沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --高沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --流失会员
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --会员生命周期
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'开卡员工
     t1.OPEN_CARD_PHMC_CODE,           --'开卡门店编码
     t1.OPEN_CARD_PHMC_NAME,           --'开卡门店名称
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'开卡门店开业日期
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'开卡门店是否医保门店
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'开卡门店产权归属
     t1.OPEN_CARD_COMPANY_CODE,        --'开卡公司编码
     t1.OPEN_CARD_COMPANY_NAME,        --'开卡公司名称
     t1.OPEN_CARD_DEPRT_CODE,          --'开卡门管部编码
     t1.OPEN_CARD_DEPRT_NAME,          --'开卡门管部名称
     t1.OPEN_CARD_DIST_CODE,           --'开卡片区编码
     t1.OPEN_CARD_DIST_NAME,           --'开卡片区名称
     --t6.MAIN_CNSM_PHMC_CODE,           --'主消费门店编码 --注释by 宁颖琦 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'主消费门店名称
     --t6.MAIN_CNSM_COMPANY_CODE,        --'主消费公司编码
     --t6.MAIN_CNSM_COMPANY_NAME,        --'主消费公司名称
     --t6.MAIN_CNSM_DEPRT_CODE,          --'主消费门管部编码
     --t6.MAIN_CNSM_DEPRT_NAME,          --'主消费门管部名称
     --t6.MAIN_CNSM_DIST_CODE,           --'主消费片区编码
     --t6.MAIN_CNSM_DIST_NAME,           --'主消费片区名称--注释by 宁颖琦 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --慢病建档时间     
     t8.LAST_BP_TIME,                  --最近一次量血压时间
     t8.LAST_BS_TIME,                  --最近一次测血糖时间
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'线下首次消费门店编码
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'线下首次消费门店名称
     t3.OFFLINE_FST_CNSM_DATE,         --'线下首次消费时间
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'线下首次消费金额
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'线下最近一次消费门店编码
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'线下最近一次消费门店名称
     t3.OFFLINE_LAST_CNSM_DATE,         --'线下最近一次消费时间
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'线下最近一次销售金额
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --线下最近三个月消费金额
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --线下最近三个月毛利率
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --线下最近三个月消费次数
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --线下最近1年消费金额
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --线下最近1年毛利率
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --线下最近1年消费次数
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --线下累计消费金额
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --线下累计消费毛利金额
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES,   --线下累计消费次数
	 t4.R_YEAR_CNSM_GOODS_CODE,         --'近一年最常购买药品编码
     t4.R_YEAR_CNSM_GOODS_NAME,         --'近一年最常购买药品名称
     t4.R_YEAR_CNSM_CATE_CODE,          --'近一年最常购买品类编码（二级）
     t4.R_YEAR_CNSM_CATE_NAME,          --'近一年最常购买品类名称（二级）
     t4.R_YEAR_TIME_PREFER,             --'近一年线下购药时间段偏好
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'近一年是否医保卡消费
from
(
select 
     :endTime AS DATA_DATE,        --数据日期
	 t1.MEMBER_ID,                 --会员ID
     ifnull(t2.IS_ACTIV_MEMB,'0') as IS_ACTIV_MEMB,	           --是否活跃会员 
	 ifnull(t2.IS_ABNO_CARD,'0') as IS_ABNO_CARD,              --是否异常卡
	 ifnull(t2.IS_NEGA_CARD,'0') as   IS_NEGA_CARD  ,        --是否负卡
	 ifnull(t2.IS_EASY_MARKETING,'0') as  IS_EASY_MARKETING,     --是否易营销 
     NULL AS LIFE_CYCLE_TYPE,      --时间频次生命周期类型	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --新客无消费
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --新客有消费
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --成长期
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --成熟期
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --衰退期
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --中度沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --深度沉睡
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --完整生命周期类型 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --客户标识(0302表新客无消费)
     t1.GNDR_AGE_TYPE,            --性别年龄类型
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --客单价
     t2.UNIT_PRI_TYPE,            --客单价类型
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --毛利率
     t2.GROS_MARGIN_TYPE,         --毛利率类型
     t3.LAST_TIME_CUNSU_DATE,     --最后一次消费时间
	 t3.R_MAX_SALE_INTERVAL,       --统计最大消费间距
     t3.R_MIN_SALE_INTERVAL,       --统计最小消费间距
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --平均消费间隔间距
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --未消费时间长
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --是否支付宝支付
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --是否微信支付
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --是否刷卡支付
	 --t5.values_R_1 as CLNT_CALU,          --客户价值   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --历史客户价值  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'本月销售额
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'本月毛利额
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'本月消费次数
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'上月销售额
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'上月毛利额
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'上月消费次数
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'最近三个月销售额
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'最近三个月毛利额
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'最近三个月消费次数
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'近半年销售额
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'近半年毛利额
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'近半年消费次数
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'近一年销售额
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'近一年毛利额
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'近一年消费次数
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'上一年销售额
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'上一年毛利额
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'上一年消费次数
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'累计销售额
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'累计毛利额
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'累计消费次数
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --慢病会员标识
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --近一年慢病品类购买次数
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --累计慢病品类购买次数
     --t5.value_level as CLNT_CALU_LEVEL,--客户价值类型 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --首次消费时间
     t1.GNDR,                          --'性别
     t1.BIRT_DATE,                     --'出生日期
     t1.BELONE_PVNC_NAME,              --'所属省份
     t1.BELONE_CITY_NAME,              --'所属市
     t1.BELONE_COUNTY_NAME,            --'所属区县
     t1.MEMB_CARD_STATE,               --'会员卡状态
     t1.MEMB_SOUR,                     --'会员来源
     t1.IS_WECHAT,                     --'是否绑定微信会员
     t1.IS_ALIPAY,                     --'是否绑定支付宝会员
     t1.IS_YF,                         --'是否关注益丰大药房
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --未消费顾客
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --低活跃度
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --忠诚
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --高活跃度
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --低沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --高沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --流失会员
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --会员生命周期
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'开卡员工
     t1.OPEN_CARD_PHMC_CODE,           --'开卡门店编码
     t1.OPEN_CARD_PHMC_NAME,           --'开卡门店名称
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'开卡门店开业日期
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'开卡门店是否医保门店
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'开卡门店产权归属
     t1.OPEN_CARD_COMPANY_CODE,        --'开卡公司编码
     t1.OPEN_CARD_COMPANY_NAME,        --'开卡公司名称
     t1.OPEN_CARD_DEPRT_CODE,          --'开卡门管部编码
     t1.OPEN_CARD_DEPRT_NAME,          --'开卡门管部名称
     t1.OPEN_CARD_DIST_CODE,           --'开卡片区编码
     t1.OPEN_CARD_DIST_NAME,           --'开卡片区名称
     --t6.MAIN_CNSM_PHMC_CODE,           --'主消费门店编码 --注释by 宁颖琦 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'主消费门店名称
     --t6.MAIN_CNSM_COMPANY_CODE,        --'主消费公司编码
     --t6.MAIN_CNSM_COMPANY_NAME,        --'主消费公司名称
     --t6.MAIN_CNSM_DEPRT_CODE,          --'主消费门管部编码
     --t6.MAIN_CNSM_DEPRT_NAME,          --'主消费门管部名称
     --t6.MAIN_CNSM_DIST_CODE,           --'主消费片区编码
     --t6.MAIN_CNSM_DIST_NAME,           --'主消费片区名称--注释by 宁颖琦 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --慢病建档时间     
     t8.LAST_BP_TIME,                  --最近一次量血压时间
     t8.LAST_BS_TIME,                  --最近一次测血糖时间
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'线下首次消费门店编码
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'线下首次消费门店名称
     t3.OFFLINE_FST_CNSM_DATE,         --'线下首次消费时间
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'线下首次消费金额
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'线下最近一次消费门店编码
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'线下最近一次消费门店名称
     t3.OFFLINE_LAST_CNSM_DATE,         --'线下最近一次消费时间
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'线下最近一次销售金额
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --线下最近三个月消费金额
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --线下最近三个月毛利率
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --线下最近三个月消费次数
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --线下最近1年消费金额
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --线下最近1年毛利率
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --线下最近1年消费次数
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --线下累计消费金额
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --线下累计消费毛利金额
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES ,  --线下累计消费次数	  
     t4.R_YEAR_CNSM_GOODS_CODE,         --'近一年最常购买药品编码
     t4.R_YEAR_CNSM_GOODS_NAME,         --'近一年最常购买药品名称
     t4.R_YEAR_CNSM_CATE_CODE,          --'近一年最常购买品类编码（二级）
     t4.R_YEAR_CNSM_CATE_NAME,          --'近一年最常购买品类名称（二级）
     t4.R_YEAR_TIME_PREFER,             --'近一年线下购药时间段偏好
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'近一年是否医保卡消费
from  :var_base                       t1 
left join  :var_total_comsp           t2  on t1.member_id=t2.member_id
left join  :var_line_comsp            t3  on t1.member_id=t3.member_id
left join  :var_goods_payMode         t4  on t1.member_id=t4.member_id 
--left join  dm.memb_value_model_result t5  on t1.member_id=t5.member_id and t5.update_date = add_days(TO_DATE(:endTime, 'yyyymmdd'), 1)
--left join  :var_main_comsp            t6  on t1.member_id=t6.member_id --注释by宁颖琦 20191023
left join  DS_ZT.ZT_CHRONIC_BASELINE  t7  on T1.member_id=t7.customer_id
left join  :var_bp_bs                 t8  on t1.member_id=t8.customer_id 
left join  :var_agg                   t9  on t1.member_id=t9.member_id 
)t1
left join :var_agg                   t2  on t1.member_id=t2.member_id 
;






var_total=
select 
     :endTime AS DATA_DATE,        --数据日期
	 t1.MEMBER_ID,                 --会员ID
     ifnull(t2.IS_ACTIV_MEMB,'0') as IS_ACTIV_MEMB,	           --是否活跃会员 
	 ifnull(t2.IS_ABNO_CARD,'0') as IS_ABNO_CARD,              --是否异常卡
	 ifnull(t2.IS_NEGA_CARD,'0') as   IS_NEGA_CARD  ,        --是否负卡
	 ifnull(t2.IS_EASY_MARKETING,'0') as  IS_EASY_MARKETING,     --是否易营销 
     NULL AS LIFE_CYCLE_TYPE,      --时间频次生命周期类型	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --新客无消费
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --新客有消费
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --成长期
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --成熟期
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --衰退期
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --中度沉睡
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --深度沉睡
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --完整生命周期类型 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --客户标识(0302表新客无消费)
     t1.GNDR_AGE_TYPE,            --性别年龄类型
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --客单价
     t2.UNIT_PRI_TYPE,            --客单价类型
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --毛利率
     t2.GROS_MARGIN_TYPE,         --毛利率类型
     t3.LAST_TIME_CUNSU_DATE,     --最后一次消费时间
	 t3.R_MAX_SALE_INTERVAL,       --统计最大消费间距
     t3.R_MIN_SALE_INTERVAL,       --统计最小消费间距
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --平均消费间隔间距
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --未消费时间长
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --是否支付宝支付
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --是否微信支付
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --是否刷卡支付
	 --t5.values_R_1 as CLNT_CALU,          --客户价值   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --历史客户价值  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'本月销售额
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'本月毛利额
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'本月消费次数
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'上月销售额
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'上月毛利额
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'上月消费次数
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'最近三个月销售额
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'最近三个月毛利额
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'最近三个月消费次数
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'近半年销售额
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'近半年毛利额
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'近半年消费次数
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'近一年销售额
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'近一年毛利额
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'近一年消费次数
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'上一年销售额
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'上一年毛利额
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'上一年消费次数
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'累计销售额
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'累计毛利额
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'累计消费次数
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --慢病会员标识
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --近一年慢病品类购买次数
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --累计慢病品类购买次数
     --t5.value_level as CLNT_CALU_LEVEL,--客户价值类型 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --首次消费时间
     t1.GNDR,                          --'性别
     t1.BIRT_DATE,                     --'出生日期
     t1.BELONE_PVNC_NAME,              --'所属省份
     t1.BELONE_CITY_NAME,              --'所属市
     t1.BELONE_COUNTY_NAME,            --'所属区县
     t1.MEMB_CARD_STATE,               --'会员卡状态
     t1.MEMB_SOUR,                     --'会员来源
     t1.IS_WECHAT,                     --'是否绑定微信会员
     t1.IS_ALIPAY,                     --'是否绑定支付宝会员
     t1.IS_YF,                         --'是否关注益丰大药房
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --未消费顾客
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --低活跃度
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --忠诚
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --高活跃度
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --低沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --高沉睡会员
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --流失会员
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --会员生命周期
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'开卡员工
     t1.OPEN_CARD_PHMC_CODE,           --'开卡门店编码
     t1.OPEN_CARD_PHMC_NAME,           --'开卡门店名称
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'开卡门店开业日期
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'开卡门店是否医保门店
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'开卡门店产权归属
     t1.OPEN_CARD_COMPANY_CODE,        --'开卡公司编码
     t1.OPEN_CARD_COMPANY_NAME,        --'开卡公司名称
     t1.OPEN_CARD_DEPRT_CODE,          --'开卡门管部编码
     t1.OPEN_CARD_DEPRT_NAME,          --'开卡门管部名称
     t1.OPEN_CARD_DIST_CODE,           --'开卡片区编码
     t1.OPEN_CARD_DIST_NAME,           --'开卡片区名称
     --t6.MAIN_CNSM_PHMC_CODE,           --'主消费门店编码 --注释by 宁颖琦 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'主消费门店名称
     --t6.MAIN_CNSM_COMPANY_CODE,        --'主消费公司编码
     --t6.MAIN_CNSM_COMPANY_NAME,        --'主消费公司名称
     --t6.MAIN_CNSM_DEPRT_CODE,          --'主消费门管部编码
     --t6.MAIN_CNSM_DEPRT_NAME,          --'主消费门管部名称
     --t6.MAIN_CNSM_DIST_CODE,           --'主消费片区编码
     --t6.MAIN_CNSM_DIST_NAME,           --'主消费片区名称--注释by 宁颖琦 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --慢病建档时间     
     t8.LAST_BP_TIME,                  --最近一次量血压时间
     t8.LAST_BS_TIME,                  --最近一次测血糖时间
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'线下首次消费门店编码
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'线下首次消费门店名称
     t3.OFFLINE_FST_CNSM_DATE,         --'线下首次消费时间
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'线下首次消费金额
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'线下最近一次消费门店编码
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'线下最近一次消费门店名称
     t3.OFFLINE_LAST_CNSM_DATE,         --'线下最近一次消费时间
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'线下最近一次销售金额
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --线下最近三个月消费金额
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --线下最近三个月毛利率
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --线下最近三个月消费次数
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --线下最近1年消费金额
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --线下最近1年毛利率
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --线下最近1年消费次数
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --线下累计消费金额
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --线下累计消费毛利金额
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES ,  --线下累计消费次数	  
     t4.R_YEAR_CNSM_GOODS_CODE,         --'近一年最常购买药品编码
     t4.R_YEAR_CNSM_GOODS_NAME,         --'近一年最常购买药品名称
     t4.R_YEAR_CNSM_CATE_CODE,          --'近一年最常购买品类编码（二级）
     t4.R_YEAR_CNSM_CATE_NAME,          --'近一年最常购买品类名称（二级）
     t4.R_YEAR_TIME_PREFER,             --'近一年线下购药时间段偏好
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'近一年是否医保卡消费
from  :var_base                       t1 
left join  :var_total_comsp           t2  on t1.member_id=t2.member_id
left join  :var_line_comsp            t3  on t1.member_id=t3.member_id
left join  :var_goods_payMode         t4  on t1.member_id=t4.member_id 
--left join  dm.memb_value_model_result t5  on t1.member_id=t5.member_id and t5.update_date = add_days(TO_DATE(:endTime, 'yyyymmdd'), 1)
--left join  :var_main_comsp            t6  on t1.member_id=t6.member_id --注释by宁颖琦 20191023
left join  DS_ZT.ZT_CHRONIC_BASELINE  t7  on T1.member_id=t7.customer_id
left join  :var_bp_bs                 t8  on t1.member_id=t8.customer_id 
left join  :var_agg                   t9  on t1.member_id=t9.member_id 
;


-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 

-------------------------------------日志参数begin--------------------------------
var_sql_step  :=9;                     --SQL步骤
var_log_begin_time  :=current_timestamp;   --开始时间
var_step_comments :='插入最终结果';                --步骤说明

-------------------------------------日志参数end----------------------------------
delete  from DM.FACT_MEMBER_CNT_INFO  where  DATA_DATE =   TO_DATE(:endTime,'yyyymmdd');
insert into DM.FACT_MEMBER_CNT_INFO
(
      DATA_DATE,--数据日期
      MEMBER_ID,--基础会员编号
      OPEN_CARD_DAYS,--开卡天数
      IS_OLD_MEMB,--是否老会员
      IS_ACTIV_MEMB,--是否活跃会员
      IS_EFFE_MEMB,--是否有效会员
      IS_ABNO_CARD,--是否异常卡
      IS_NEGA_CARD,--是否负卡
      IS_EASY_MARKETING,--是否易营销
      LIFE_CYCLE_TYPE,--时间频次生命周期类型
      ENTI_LIFE_CYCLE_TYPE,--完整生命周期类型
      MEMBER_TYPE,--客户标识
      GNDR_AGE_TYPE,--性别年龄类型
      UNIT_PRI,--客单价
      UNIT_PRI_TYPE,--客单价类型
      GROS_MARGIN,--毛利率
      GROS_MARGIN_TYPE,--毛利率类型
      LAST_TIME_CUNSU_DATE,--最后一次消费时间
      R_MAX_SALE_INTERVAL,--统计最大消费间距
      R_MIN_SALE_INTERVAL,--统计最小消费间距
      R_AVG_SALE_INTERVAL,--统计平均消费间距
      NO_CONSU_TIME_LONG_MEMB_BEHAV,--未消费时间长
      IS_ALI_PAY_MEMB_BEHAV,--是否支付宝支付
      IS_WX_PAY_MEMB_BEHAV,--是否微信支付
      IS_BANKCARD_PAY_MEMB_BEHAV,--是否刷卡支付
      CLNT_CALU,--客户价值
      CLNT_CALU_HIS,--客户历史价值
      M_SALE_AMT,--本月销售额
      M_GROSS_AMT,--本月毛利额
      M_SONSU_TIMES,--本月消费次数
      L_M_SALE_AMT,--上月销售额
      L_M_GROSS_AMT,--上月毛利额
      L_M_SONSU_TIMES,--上月消费次数
      Q_SALE_AMT,--最近三个月销售额
      Q_GROSS_AMT,--最近三个月毛利额
      Q_SONSU_TIMES,--最近三个月消费次数
      R_HALF_SALE_AMT,--近半年销售额
      R_HALF_GROSS_AMT,--近半年毛利额
      R_HALF_SONSU_TIMES,--近半年消费次数
      R_YEAR_SALE_AMT,--近一年销售额
      R_YEAR_GROSS_AMT,--近一年毛利额
      R_YEAR_SONSU_TIMES,--近一年消费次数
      R_LAST_YEAR_SALE_AMT,--上一年销售额
      R_LAST_YEAR_GROSS_AMT,--上一年毛利额
      R_LAST_YEAR_SONSU_TIMES,--上一年消费次数
      R_ALL_SALE_AMT,--累计销售额
      R_ALL_GROSS_AMT,--累计毛利额
      R_ALL_SONSU_TIMES,--累计消费次数
      NCD_TYPE,--慢病会员标识
      NCD_CNT,--近一年慢病品类购买次数
      R_ALL_NCD_CNT,--累计慢病品类购买次数
      CLNT_CALU_LEVEL,--客户价值类型
      FST_CUNSU_DATE,--首次消费时间
      GNDR,--性别
      --BIRT_DATE,--出生日期 注释by 20191023 begin
      --BELONE_PVNC_NAME,--所属省份
      --BELONE_CITY_NAME,--所属市
      --BELONE_COUNTY_NAME,--所属区县
      MEMB_CARD_STATE,--会员卡状态
      MEMB_SOUR,--会员来源
      IS_WECHAT,--是否绑定微信会员
      IS_ALIPAY,--是否绑定支付宝会员
      IS_YF,--是否关注益丰大药房
      MEMB_LIFE_CYCLE,--会员生命周期
      OPEN_CARD_TIME,--开卡时间
      --OPEN_CARD_EMPE,--开卡员工
      --OPEN_CARD_PHMC_CODE,--开卡门店编码
      --OPEN_CARD_PHMC_NAME,--开卡门店名称
      --OPEN_CARD_PHMC_STAR_BUSI,--开卡门店开业日期
      --OPEN_CARD_PHMC_IS_MEDI_INSU,--开卡门店是否医保门店
      --OPEN_CARD_PHMC_PROR_ATTR,--开卡门店产权归属
      --OPEN_CARD_COMPANY_CODE,--开卡公司编码
      --OPEN_CARD_COMPANY_NAME,--开卡公司名称
      --OPEN_CARD_DEPRT_CODE,--开卡门管部编码
      --OPEN_CARD_DEPRT_NAME,--开卡门管部名称
      --OPEN_CARD_DIST_CODE,--开卡片区编码
      --OPEN_CARD_DIST_NAME,--开卡片区名称
      --MAIN_CNSM_PHMC_CODE,--主消费门店编码
      --MAIN_CNSM_PHMC_NAME,--主消费门店名称
      --MAIN_CNSM_COMPANY_CODE,--主消费公司编码
      --MAIN_CNSM_COMPANY_NAME,--主消费公司名称
      --MAIN_CNSM_DEPRT_CODE,--主消费门管部编码
     -- MAIN_CNSM_DEPRT_NAME,--主消费门管部名称
      --MAIN_CNSM_DIST_CODE,--主消费片区编码
      --MAIN_CNSM_DIST_NAME,--主消费片区名称 --出生日期 注释by 20191023 end
      NCD_CREA_TIME,--慢病建档时间
      LAST_BP_TIME,--最近一次量血压时间
      LAST_BS_TIME,--最近一次测血糖时间
      OFFLINE_FST_CNSM_PHMC_CODE,--线下首次消费门店编码
      OFFLINE_FST_CNSM_PHMC_NAME,--线下首次消费门店名称
      OFFLINE_FST_CNSM_DATE,--线下首次消费时间
      OFFLINE_FST_CNSM_AMT,--线下首次消费金额
      OFFLINE_LAST_CNSM_PHMC_CODE,--线下最近一次消费门店编码
      OFFLINE_LAST_CNSM_PHMC_NAME,--线下最近一次消费门店名称
      OFFLINE_LAST_CNSM_DATE,--线下最近一次消费时间
      OFFLINE_LAST_CNSM_AMT,--线下最近一次销售金额
      OFFLINE_Q_CNSM_AMT,--线下最近三个月消费金额
      OFFLINE_Q_GROSS_RATE,--线下最近三个月毛利率
      OFFLINE_Q_CNSM_TIMES,--线下最近三个月消费次数
      OFFLINE_Y_CNSM_AMT,--线下最近一年消费金额
      OFFLINE_Y_GROSS_RATE,--线下最近一年毛利率
      OFFLINE_Y_CNSM_TIMES,--线下最近一年消费次数
      OFFLINE_TOTAL_CNSM_AMT,--线下累计消费金额
      OFFLINE_TOTAL_GROS_AMT,--线下累计毛利额
      OFFLINE_TOTAL_CNSM_TIMES,--线下累计消费次数
      R_YEAR_CNSM_GOODS_CODE,--近一年最常购买药品编码
      R_YEAR_CNSM_GOODS_NAME,--近一年最常购买药品名称
      R_YEAR_CNSM_CATE_CODE,--近一年最常购买品类编码（二级）
      R_YEAR_CNSM_CATE_NAME,--近一年最常购买品类名称（二级）
      R_YEAR_TIME_PREFER,--近一年线下购药时间段偏好
      R_YEAR_IS_MEDI,--近一年是否医保卡消费
      LOAD_TIME      --导入时间
)
select 
     DATA_DATE,        --数据日期
	 MEMBER_ID,                 --会员ID
     days_between(OPEN_CARD_TIME,:endTime) as OPEN_CARD_DAYS,--开卡天数
     CASE WHEN days_between(OPEN_CARD_TIME,:endTime) > 365  then  '1'  else '0' END AS IS_OLD_MEMB,              --是否老会员
     IS_ACTIV_MEMB,	           --是否活跃会员
     CASE WHEN days_between(OPEN_CARD_TIME,TO_DATE(:endTime,'yyyymmdd')) >= 365 and R_YEAR_SONSU_TIMES >= 1 then '1'
          WHEN days_between(OPEN_CARD_TIME,TO_DATE(:endTime,'yyyymmdd')) < 365 and R_YEAR_SONSU_TIMES >= 2 then '1'
          ELSE '0' 
     END IS_EFFE_MEMB_STSC,        --是否有效会员 
	 IS_ABNO_CARD,              --是否异常卡
	 IS_NEGA_CARD,        --是否负卡
	 IS_EASY_MARKETING,     --是否易营销 
     LIFE_CYCLE_TYPE,      --时间频次生命周期类型	 
     ENTI_LIFE_CYCLE_TYPE,    --完整生命周期类型 
     MEMBER_TYPE,              --客户标识(0302表新客无消费)
     GNDR_AGE_TYPE,            --性别年龄类型
     UNIT_PRI,        --客单价
     UNIT_PRI_TYPE,            --客单价类型
     GROS_MARGIN,      --毛利率
     GROS_MARGIN_TYPE,         --毛利率类型
     LAST_TIME_CUNSU_DATE,     --最后一次消费时间
	 R_MAX_SALE_INTERVAL,       --统计最大消费间距
     R_MIN_SALE_INTERVAL,       --统计最小消费间距
     R_AVG_SALE_INTERVAL,       --平均消费间隔间距
     NO_CONSU_TIME_LONG_MEMB_BEHAV, --未消费时间长
     IS_ALI_PAY_MEMB_BEHAV,         --是否支付宝支付
     IS_WX_PAY_MEMB_BEHAV,          --是否微信支付
     IS_BANKCARD_PAY_MEMB_BEHAV,    --是否刷卡支付
     CLNT_CALU,
     CLNT_CALU_HIS,
     M_SALE_AMT,                    --'本月销售额
     M_GROSS_AMT,                    --'本月毛利额
     M_SONSU_TIMES,                 --'本月消费次数
     L_M_SALE_AMT,                  --'上月销售额
     L_M_GROSS_AMT,                 --'上月毛利额
     L_M_SONSU_TIMES,               --'上月消费次数
     Q_SALE_AMT,                    --'最近三个月销售额
     Q_GROSS_AMT,                   --'最近三个月毛利额
     Q_SONSU_TIMES,                 --'最近三个月消费次数
     R_HALF_SALE_AMT,               --'近半年销售额
     R_HALF_GROSS_AMT,              --'近半年毛利额
     R_HALF_SONSU_TIMES,            --'近半年消费次数
     R_YEAR_SALE_AMT,               --'近一年销售额
     R_YEAR_GROSS_AMT,              --'近一年毛利额
     R_YEAR_SONSU_TIMES,            --'近一年消费次数
     R_LAST_YEAR_SALE_AMT,          --'上一年销售额
     R_LAST_YEAR_GROSS_AMT,         --'上一年毛利额
     R_LAST_YEAR_SONSU_TIMES,       --'上一年消费次数
     R_ALL_SALE_AMT,                --'累计销售额
     R_ALL_GROSS_AMT,               --'累计毛利额
     R_ALL_SONSU_TIMES,             --'累计消费次数
     NCD_TYPE,                      --慢病会员标识
     NCD_CNT,             --近一年慢病品类购买次数
     R_ALL_NCD_CNT, --累计慢病品类购买次数
     CLNT_CALU_LEVEL,
     FST_CUNSU_DATE,                --首次消费时间
     GNDR,                          --'性别
     --BIRT_DATE,                     --'出生日期  --出生日期 注释by 20191023 begin
     --BELONE_PVNC_NAME,              --'所属省份
     --BELONE_CITY_NAME,              --'所属市
     --BELONE_COUNTY_NAME,            --'所属区县
     MEMB_CARD_STATE,               --'会员卡状态
     MEMB_SOUR,                     --'会员来源
     IS_WECHAT,                     --'是否绑定微信会员
     IS_ALIPAY,                     --'是否绑定支付宝会员
     IS_YF,                         --'是否关注益丰大药房
     MEMB_LIFE_CYCLE,          		--会员生命周期
     OPEN_CARD_TIME,                --'开卡时间
    -- OPEN_CARD_EMPE,                --'开卡员工 
     --OPEN_CARD_PHMC_CODE,           --'开卡门店编码
     --OPEN_CARD_PHMC_NAME,           --'开卡门店名称
     --OPEN_CARD_PHMC_STAR_BUSI,      --'开卡门店开业日期
     --OPEN_CARD_PHMC_IS_MEDI_INSU,   --'开卡门店是否医保门店
     --OPEN_CARD_PHMC_PROR_ATTR,      --'开卡门店产权归属
     --OPEN_CARD_COMPANY_CODE,        --'开卡公司编码
     --OPEN_CARD_COMPANY_NAME,        --'开卡公司名称
     --OPEN_CARD_DEPRT_CODE,          --'开卡门管部编码
     --OPEN_CARD_DEPRT_NAME,          --'开卡门管部名称
     --OPEN_CARD_DIST_CODE,           --'开卡片区编码
     --OPEN_CARD_DIST_NAME,           --'开卡片区名称
     --MAIN_CNSM_PHMC_CODE,           --'主消费门店编码
     --MAIN_CNSM_PHMC_NAME,           --'主消费门店名称
     --MAIN_CNSM_COMPANY_CODE,        --'主消费公司编码
     --MAIN_CNSM_COMPANY_NAME,        --'主消费公司名称
     --MAIN_CNSM_DEPRT_CODE,          --'主消费门管部编码
     --MAIN_CNSM_DEPRT_NAME,          --'主消费门管部名称
     --MAIN_CNSM_DIST_CODE,           --'主消费片区编码
     --MAIN_CNSM_DIST_NAME,           --'主消费片区名称 --出生日期 注释by 20191023 end
     NCD_CREA_TIME ,                --慢病建档时间     
     LAST_BP_TIME,                  --最近一次量血压时间
     LAST_BS_TIME,                  --最近一次测血糖时间
     OFFLINE_FST_CNSM_PHMC_CODE,    --'线下首次消费门店编码
     OFFLINE_FST_CNSM_PHMC_NAME,    --'线下首次消费门店名称
     OFFLINE_FST_CNSM_DATE,         --'线下首次消费时间
     OFFLINE_FST_CNSM_AMT, --'线下首次消费金额
     OFFLINE_LAST_CNSM_PHMC_CODE,   --'线下最近一次消费门店编码
     OFFLINE_LAST_CNSM_PHMC_NAME,   --'线下最近一次消费门店名称
     OFFLINE_LAST_CNSM_DATE,         --'线下最近一次消费时间
     OFFLINE_LAST_CNSM_AMT,          --'线下最近一次销售金额
     OFFLINE_Q_CNSM_AMT,           --线下最近三个月消费金额
     OFFLINE_Q_GROSS_RATE, --线下最近三个月毛利率
     OFFLINE_Q_CNSM_TIMES,          --线下最近三个月消费次数
     OFFLINE_Y_CNSM_AMT,           --线下最近1年消费金额
     OFFLINE_Y_GROSS_RATE, --线下最近1年毛利率
     OFFLINE_Y_CNSM_TIMES,           --线下最近1年消费次数
	 OFFLINE_TOTAL_CNSM_AMT ,  --线下累计消费金额
	 OFFLINE_TOTAL_GROS_AMT ,  --线下累计消费毛利金额
     OFFLINE_TOTAL_CNSM_TIMES ,  --线下累计消费次数	  
     R_YEAR_CNSM_GOODS_CODE,         --'近一年最常购买药品编码
     R_YEAR_CNSM_GOODS_NAME,         --'近一年最常购买药品名称
     R_YEAR_CNSM_CATE_CODE,          --'近一年最常购买品类编码（二级）
     R_YEAR_CNSM_CATE_NAME,          --'近一年最常购买品类名称（二级）
     R_YEAR_TIME_PREFER,             --'近一年线下购药时间段偏好
     R_YEAR_IS_MEDI,    --'近一年是否医保卡消费
     current_timestamp  AS LOAD_TIME                --导入时间
from  :var_total
;

-------------------------------------执行SQL写入日志开始---------------------------			 
var_log_end_time :=current_timestamp;
insert into DS_AMS.FACT_MNGE_PROC_LOG_INFO 
( OBJECT_NAME  ,
  BEGIN_TIME  ,
  END_TIME  ,
  STEP_CNENT  ,
  STEP_SEQ,
  STEP_DESC,
  STSC_DATE  ) 
VALUES(:var_sp_name,:var_log_begin_time,:var_log_end_time,'',:var_sql_step, :var_step_comments,:var_stsc_date);
COMMIT;
-------------------------------------执行SQL写入日志结束--------------------------- 
commit;
end;
