CREATE PROCEDURE DM.SP_MEMBER_CNT_INFO_V3(IN endTime NVARCHAR(8),MONTH_QTY int)
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
-------------�޸���ʷ-----------------------------------------
--ʱ��:20190814
--�汾:��Ա��ǩ��3
--������Ա:���Ρ�Ѧ�ޡ���ӱ��
--Ŀ�ģ�������Ա��ǩ���Ż��ɱ�ǩ�ֶ�
-------------�޸���ʷ-----------------------------------------
--20190821   Ѧ��      ������־
--20190905   Ѧ��      �л�����ʽ�Ļ�Ա��ǩ��
--20191023	 ��ӱ��	   ɾ�������ֶ� �ڣ�var_base ��var_main_comsp �в��ֻ�ȫ���ֶΣ�

--���������ͼ����ʼ���
var_offline_begin_time nvarchar(8) :=to_char( add_days(last_day(add_months(to_date(:endTime, 'yyyymmdd'), -:MONTH_QTY)),1),'yyyymmdd') ;
var_memb_static_time nvarchar(8) := to_char(add_days(add_years(to_date(:endTime,'yyyymmdd'), -1),1),'yyyymmdd');
var_main_info_time nvarchar(8) := to_char(add_days(add_months(to_date(:endTime,'yyyymmdd'),-6),1),'yyyymmdd');


-------------------------------------��־������ʼ��begin--------------------------
var_sp_name varchar(50) :='DM.SP_MEMBER_CNT_INFO_V3';--sp����
var_stsc_date nvarchar(20) :=to_char(current_date,'yyyymmdd') ;--��������(Ƶ�ʵ���)
var_log_begin_time LONGDATE ;   				--��ʼʱ��
var_log_end_time LONGDATE;                      --SQL����ʱ��
var_sql_step int ;                          	--SQL����
var_step_comments varchar(500);             	--����˵��
-------------------------------------��־������ʼ��end----------------------------

begin
--step1:��ȡ��Ա������Ϣ

-------------------------------------��־����begin--------------------------------
var_sql_step  :=1;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ��Ա�����ֶ���Ϣ';                --����˵��

-------------------------------------��־����end----------------------------------
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
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_LABEL_BASE_INFO" --�޸�by ��ӱ�� 20191023
/*	 ע��by ��ӱ�� 20191023
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_LABEL_BASE_INFO"(
PLACEHOLDER."$$BeginTime$$" => :var_offline_begin_time,
PLACEHOLDER."$$EndTime$$" => :endTime)
*/
;

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 


--step2:��ȡ��Ա������������

-------------------------------------��־����begin--------------------------------
var_sql_step  :=2;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ�������ֶ���Ϣ';                --����˵��

-------------------------------------��־����end----------------------------------

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

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 



--step3:��ȡ��Ա�״�+�����������;���+��С+ƽ��������;�����״�+����������ڡ��ŵꡢ���

-------------------------------------��־����begin--------------------------------
var_sql_step  :=3;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ�����ֶ���Ϣ';                --����˵��

-------------------------------------��־����end----------------------------------

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

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 


--step4:��ȡ��Ա֧����ʽ����ǩ

-------------------------------------��־����begin--------------------------------
var_sql_step  :=4;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡͳ�Ƹ���ǩ';                --����˵��

-------------------------------------��־����end----------------------------------
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

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 



--step5:��ȡ��Ա�����ѹ�˾Ƭ���Ź��ŵ�

-------------------------------------��־����begin--------------------------------
var_sql_step  :=5;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ��Ա�����ѹ�˾Ƭ���Ź��ŵ�';                --����˵��

-------------------------------------��־����end----------------------------------
/* ע��by ��ӱ�� 20191023
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
-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 


--step6:��ȡ��ԱѪ�Ǳ�ǩ


-------------------------------------��־����begin--------------------------------
var_sql_step  :=6;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ��ԱѪ�Ǳ�ǩ';                --����˵��

-------------------------------------��־����end----------------------------------
var_bp_bs=                          --t8
 SELECT
	 "CUSTOMER_ID",
	 max("LAST_BP_TIME") AS "LAST_BP_TIME",
	 max("LAST_BS_TIME") AS "LAST_BS_TIME" 
FROM "_SYS_BIC"."YF_BI.DM.CRM/CV_MEMB_NCD_INFO"(PLACEHOLDER."$$End_Time$$" => :endTime)
GROUP BY "CUSTOMER_ID"
;

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 

--step7:��ȡ��Ա���¸���ǩ

-------------------------------------��־����begin--------------------------------
var_sql_step  :=7;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='��ȡ��Ա���¸����۱�ǩ';                --����˵��

-------------------------------------��־����end----------------------------------
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

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 

--step8:�õ����ս��

-------------------------------------��־����begin--------------------------------
var_sql_step  :=8;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='�������ս��';                --����˵��

-------------------------------------��־����end----------------------------------
var_total=

select :endTime AS DATA_DATE,        --��������
	 t1.MEMBER_ID,                 --��ԱID
     t1.IS_ACTIV_MEMB,	           --�Ƿ��Ծ��Ա 
	 t1.IS_ABNO_CARD,              --�Ƿ��쳣��
	 t1.IS_NEGA_CARD  ,        --�Ƿ񸺿�
	 t1.IS_EASY_MARKETING,     --�Ƿ���Ӫ�� 
     NULL AS LIFE_CYCLE_TYPE,      --ʱ��Ƶ��������������	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --�¿�������
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --�¿�������
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --�ɳ���
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --������
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --˥����
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --��˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --�жȳ�˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --��ȳ�˯
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --���������������� 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --�ͻ���ʶ(0302���¿�������)
     t1.GNDR_AGE_TYPE,            --�Ա���������
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --�͵���
     t2.UNIT_PRI_TYPE,            --�͵�������
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --ë����
     t2.GROS_MARGIN_TYPE,         --ë��������
     t3.LAST_TIME_CUNSU_DATE,     --���һ������ʱ��
	 t3.R_MAX_SALE_INTERVAL,       --ͳ��������Ѽ��
     t3.R_MIN_SALE_INTERVAL,       --ͳ����С���Ѽ��
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --ƽ�����Ѽ�����
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --δ����ʱ�䳤
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --�Ƿ�֧����֧��
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --�Ƿ�΢��֧��
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --�Ƿ�ˢ��֧��
	 --t5.values_R_1 as CLNT_CALU,          --�ͻ���ֵ   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --��ʷ�ͻ���ֵ  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'�������۶�
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'����ë����
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'�������Ѵ���
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'�������۶�
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'����ë����
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'�������Ѵ���
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'������������۶�
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'���������ë����
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'������������Ѵ���
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'���������۶�
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'������ë����
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'���������Ѵ���
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'��һ�����۶�
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'��һ��ë����
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'��һ�����Ѵ���
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'��һ�����۶�
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'��һ��ë����
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'��һ�����Ѵ���
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'�ۼ����۶�
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'�ۼ�ë����
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'�ۼ����Ѵ���
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --������Ա��ʶ
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --��һ������Ʒ�๺�����
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --�ۼ�����Ʒ�๺�����
     --t5.value_level as CLNT_CALU_LEVEL,--�ͻ���ֵ���� 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --�״�����ʱ��
     t1.GNDR,                          --'�Ա�
     t1.BIRT_DATE,                     --'��������
     t1.BELONE_PVNC_NAME,              --'����ʡ��
     t1.BELONE_CITY_NAME,              --'������
     t1.BELONE_COUNTY_NAME,            --'��������
     t1.MEMB_CARD_STATE,               --'��Ա��״̬
     t1.MEMB_SOUR,                     --'��Ա��Դ
     t1.IS_WECHAT,                     --'�Ƿ��΢�Ż�Ա
     t1.IS_ALIPAY,                     --'�Ƿ��֧������Ա
     t1.IS_YF,                         --'�Ƿ��ע����ҩ��
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --δ���ѹ˿�
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --�ͻ�Ծ��
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --�ҳ�
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --�߻�Ծ��
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --�ͳ�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --�߳�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --��ʧ��Ա
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --��Ա��������
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'����Ա��
     t1.OPEN_CARD_PHMC_CODE,           --'�����ŵ����
     t1.OPEN_CARD_PHMC_NAME,           --'�����ŵ�����
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'�����ŵ꿪ҵ����
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'�����ŵ��Ƿ�ҽ���ŵ�
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'�����ŵ��Ȩ����
     t1.OPEN_CARD_COMPANY_CODE,        --'������˾����
     t1.OPEN_CARD_COMPANY_NAME,        --'������˾����
     t1.OPEN_CARD_DEPRT_CODE,          --'�����Źܲ�����
     t1.OPEN_CARD_DEPRT_NAME,          --'�����Źܲ�����
     t1.OPEN_CARD_DIST_CODE,           --'����Ƭ������
     t1.OPEN_CARD_DIST_NAME,           --'����Ƭ������
     --t6.MAIN_CNSM_PHMC_CODE,           --'�������ŵ���� --ע��by ��ӱ�� 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'�������ŵ�����
     --t6.MAIN_CNSM_COMPANY_CODE,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_COMPANY_NAME,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_DEPRT_CODE,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DEPRT_NAME,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DIST_CODE,           --'������Ƭ������
     --t6.MAIN_CNSM_DIST_NAME,           --'������Ƭ������--ע��by ��ӱ�� 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --��������ʱ��     
     t8.LAST_BP_TIME,                  --���һ����Ѫѹʱ��
     t8.LAST_BS_TIME,                  --���һ�β�Ѫ��ʱ��
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'�����״������ŵ����
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'�����״������ŵ�����
     t3.OFFLINE_FST_CNSM_DATE,         --'�����״�����ʱ��
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'�����״����ѽ��
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'�������һ�������ŵ����
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'�������һ�������ŵ�����
     t3.OFFLINE_LAST_CNSM_DATE,         --'�������һ������ʱ��
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'�������һ�����۽��
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --����������������ѽ��
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --�������������ë����
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --����������������Ѵ���
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --�������1�����ѽ��
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --�������1��ë����
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --�������1�����Ѵ���
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --�����ۼ����ѽ��
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --�����ۼ�����ë�����
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES,   --�����ۼ����Ѵ���
	 t4.R_YEAR_CNSM_GOODS_CODE,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_GOODS_NAME,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_CATE_CODE,          --'��һ�������Ʒ����루������
     t4.R_YEAR_CNSM_CATE_NAME,          --'��һ�������Ʒ�����ƣ�������
     t4.R_YEAR_TIME_PREFER,             --'��һ�����¹�ҩʱ���ƫ��
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'��һ���Ƿ�ҽ��������
from
(
select 
     :endTime AS DATA_DATE,        --��������
	 t1.MEMBER_ID,                 --��ԱID
     ifnull(t2.IS_ACTIV_MEMB,'0') as IS_ACTIV_MEMB,	           --�Ƿ��Ծ��Ա 
	 ifnull(t2.IS_ABNO_CARD,'0') as IS_ABNO_CARD,              --�Ƿ��쳣��
	 ifnull(t2.IS_NEGA_CARD,'0') as   IS_NEGA_CARD  ,        --�Ƿ񸺿�
	 ifnull(t2.IS_EASY_MARKETING,'0') as  IS_EASY_MARKETING,     --�Ƿ���Ӫ�� 
     NULL AS LIFE_CYCLE_TYPE,      --ʱ��Ƶ��������������	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --�¿�������
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --�¿�������
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --�ɳ���
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --������
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --˥����
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --��˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --�жȳ�˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --��ȳ�˯
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --���������������� 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --�ͻ���ʶ(0302���¿�������)
     t1.GNDR_AGE_TYPE,            --�Ա���������
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --�͵���
     t2.UNIT_PRI_TYPE,            --�͵�������
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --ë����
     t2.GROS_MARGIN_TYPE,         --ë��������
     t3.LAST_TIME_CUNSU_DATE,     --���һ������ʱ��
	 t3.R_MAX_SALE_INTERVAL,       --ͳ��������Ѽ��
     t3.R_MIN_SALE_INTERVAL,       --ͳ����С���Ѽ��
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --ƽ�����Ѽ�����
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --δ����ʱ�䳤
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --�Ƿ�֧����֧��
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --�Ƿ�΢��֧��
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --�Ƿ�ˢ��֧��
	 --t5.values_R_1 as CLNT_CALU,          --�ͻ���ֵ   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --��ʷ�ͻ���ֵ  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'�������۶�
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'����ë����
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'�������Ѵ���
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'�������۶�
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'����ë����
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'�������Ѵ���
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'������������۶�
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'���������ë����
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'������������Ѵ���
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'���������۶�
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'������ë����
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'���������Ѵ���
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'��һ�����۶�
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'��һ��ë����
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'��һ�����Ѵ���
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'��һ�����۶�
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'��һ��ë����
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'��һ�����Ѵ���
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'�ۼ����۶�
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'�ۼ�ë����
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'�ۼ����Ѵ���
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --������Ա��ʶ
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --��һ������Ʒ�๺�����
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --�ۼ�����Ʒ�๺�����
     --t5.value_level as CLNT_CALU_LEVEL,--�ͻ���ֵ���� 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --�״�����ʱ��
     t1.GNDR,                          --'�Ա�
     t1.BIRT_DATE,                     --'��������
     t1.BELONE_PVNC_NAME,              --'����ʡ��
     t1.BELONE_CITY_NAME,              --'������
     t1.BELONE_COUNTY_NAME,            --'��������
     t1.MEMB_CARD_STATE,               --'��Ա��״̬
     t1.MEMB_SOUR,                     --'��Ա��Դ
     t1.IS_WECHAT,                     --'�Ƿ��΢�Ż�Ա
     t1.IS_ALIPAY,                     --'�Ƿ��֧������Ա
     t1.IS_YF,                         --'�Ƿ��ע����ҩ��
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --δ���ѹ˿�
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --�ͻ�Ծ��
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --�ҳ�
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --�߻�Ծ��
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --�ͳ�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --�߳�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --��ʧ��Ա
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --��Ա��������
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'����Ա��
     t1.OPEN_CARD_PHMC_CODE,           --'�����ŵ����
     t1.OPEN_CARD_PHMC_NAME,           --'�����ŵ�����
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'�����ŵ꿪ҵ����
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'�����ŵ��Ƿ�ҽ���ŵ�
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'�����ŵ��Ȩ����
     t1.OPEN_CARD_COMPANY_CODE,        --'������˾����
     t1.OPEN_CARD_COMPANY_NAME,        --'������˾����
     t1.OPEN_CARD_DEPRT_CODE,          --'�����Źܲ�����
     t1.OPEN_CARD_DEPRT_NAME,          --'�����Źܲ�����
     t1.OPEN_CARD_DIST_CODE,           --'����Ƭ������
     t1.OPEN_CARD_DIST_NAME,           --'����Ƭ������
     --t6.MAIN_CNSM_PHMC_CODE,           --'�������ŵ���� --ע��by ��ӱ�� 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'�������ŵ�����
     --t6.MAIN_CNSM_COMPANY_CODE,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_COMPANY_NAME,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_DEPRT_CODE,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DEPRT_NAME,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DIST_CODE,           --'������Ƭ������
     --t6.MAIN_CNSM_DIST_NAME,           --'������Ƭ������--ע��by ��ӱ�� 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --��������ʱ��     
     t8.LAST_BP_TIME,                  --���һ����Ѫѹʱ��
     t8.LAST_BS_TIME,                  --���һ�β�Ѫ��ʱ��
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'�����״������ŵ����
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'�����״������ŵ�����
     t3.OFFLINE_FST_CNSM_DATE,         --'�����״�����ʱ��
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'�����״����ѽ��
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'�������һ�������ŵ����
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'�������һ�������ŵ�����
     t3.OFFLINE_LAST_CNSM_DATE,         --'�������һ������ʱ��
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'�������һ�����۽��
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --����������������ѽ��
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --�������������ë����
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --����������������Ѵ���
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --�������1�����ѽ��
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --�������1��ë����
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --�������1�����Ѵ���
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --�����ۼ����ѽ��
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --�����ۼ�����ë�����
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES ,  --�����ۼ����Ѵ���	  
     t4.R_YEAR_CNSM_GOODS_CODE,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_GOODS_NAME,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_CATE_CODE,          --'��һ�������Ʒ����루������
     t4.R_YEAR_CNSM_CATE_NAME,          --'��һ�������Ʒ�����ƣ�������
     t4.R_YEAR_TIME_PREFER,             --'��һ�����¹�ҩʱ���ƫ��
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'��һ���Ƿ�ҽ��������
from  :var_base                       t1 
left join  :var_total_comsp           t2  on t1.member_id=t2.member_id
left join  :var_line_comsp            t3  on t1.member_id=t3.member_id
left join  :var_goods_payMode         t4  on t1.member_id=t4.member_id 
--left join  dm.memb_value_model_result t5  on t1.member_id=t5.member_id and t5.update_date = add_days(TO_DATE(:endTime, 'yyyymmdd'), 1)
--left join  :var_main_comsp            t6  on t1.member_id=t6.member_id --ע��by��ӱ�� 20191023
left join  DS_ZT.ZT_CHRONIC_BASELINE  t7  on T1.member_id=t7.customer_id
left join  :var_bp_bs                 t8  on t1.member_id=t8.customer_id 
left join  :var_agg                   t9  on t1.member_id=t9.member_id 
)t1
left join :var_agg                   t2  on t1.member_id=t2.member_id 
;






var_total=
select 
     :endTime AS DATA_DATE,        --��������
	 t1.MEMBER_ID,                 --��ԱID
     ifnull(t2.IS_ACTIV_MEMB,'0') as IS_ACTIV_MEMB,	           --�Ƿ��Ծ��Ա 
	 ifnull(t2.IS_ABNO_CARD,'0') as IS_ABNO_CARD,              --�Ƿ��쳣��
	 ifnull(t2.IS_NEGA_CARD,'0') as   IS_NEGA_CARD  ,        --�Ƿ񸺿�
	 ifnull(t2.IS_EASY_MARKETING,'0') as  IS_EASY_MARKETING,     --�Ƿ���Ӫ�� 
     NULL AS LIFE_CYCLE_TYPE,      --ʱ��Ƶ��������������	 
     case when ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '00' --�¿�������
          when t2.R_ALL_SONSU_TIMES=1 AND t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '01' --�¿�������
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES<=8 then '02' --�ɳ���
          when t2.R_HALF_SONSU_TIMES>=2 and t2.R_ALL_SONSU_TIMES>8 then '03' --������
          when t2.R_HALF_SONSU_TIMES<2 and t2.R_ALL_SONSU_TIMES>=2 and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) then '04' --˥����
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-9) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24)
              and t2.R_ALL_SONSU_TIMES>=1 then '0501' --��˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-24) and t3.LAST_TIME_CUNSU_DATE>=ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36)
              and t2.R_ALL_SONSU_TIMES>=1 then '0502' --�жȳ�˯
          when t3.LAST_TIME_CUNSU_DATE<ADD_MONTHS(TO_DATE(:endTime,'yyyymmdd'),-36) and t2.R_ALL_SONSU_TIMES>=1 then '0503' --��ȳ�˯
          else '' 
     end ENTI_LIFE_CYCLE_TYPE,    --���������������� 
     ifnull(t2.MEMBER_TYPE,'0302')as MEMBER_TYPE,              --�ͻ���ʶ(0302���¿�������)
     t1.GNDR_AGE_TYPE,            --�Ա���������
     ifnull(t2.UNIT_PRI,0) as UNIT_PRI,        --�͵���
     t2.UNIT_PRI_TYPE,            --�͵�������
     ifnull(t2.GROS_MARGIN,0) as GROS_MARGIN,      --ë����
     t2.GROS_MARGIN_TYPE,         --ë��������
     t3.LAST_TIME_CUNSU_DATE,     --���һ������ʱ��
	 t3.R_MAX_SALE_INTERVAL,       --ͳ��������Ѽ��
     t3.R_MIN_SALE_INTERVAL,       --ͳ����С���Ѽ��
     CASE WHEN (t2.R_ALL_SONSU_TIMES-1)>0 then DAYS_BETWEEN(t3.FST_CUNSU_DATE,T3.LAST_TIME_CUNSU_DATE)/(t2.R_ALL_SONSU_TIMES-1)
     end AS R_AVG_SALE_INTERVAL,       --ƽ�����Ѽ�����
     t3.NO_CONSU_TIME_LONG_MEMB_BEHAV, --δ����ʱ�䳤
     ifnull(t4.IS_ALI_PAY_MEMB_BEHAV,0) as IS_ALI_PAY_MEMB_BEHAV,         --�Ƿ�֧����֧��
     ifnull(t4.IS_WX_PAY_MEMB_BEHAV,0) as IS_WX_PAY_MEMB_BEHAV,          --�Ƿ�΢��֧��
     ifnull(t4.IS_BANKCARD_PAY_MEMB_BEHAV,0) as IS_BANKCARD_PAY_MEMB_BEHAV,    --�Ƿ�ˢ��֧��
	 --t5.values_R_1 as CLNT_CALU,          --�ͻ���ֵ   
	 --t5.HISTORY_M  as CLNT_CALU_HIS,      --��ʷ�ͻ���ֵ  
     null as CLNT_CALU,
     null as CLNT_CALU_HIS,
     ifnull(t2.M_SALE_AMT,0) as M_SALE_AMT,                    --'�������۶�
     ifnull(t2.M_GROSS_AMT,0) as M_GROSS_AMT,                    --'����ë����
     ifnull(t2.M_SONSU_TIMES,0) as M_SONSU_TIMES,                 --'�������Ѵ���
     ifnull(t2.L_M_SALE_AMT,0) as L_M_SALE_AMT,                  --'�������۶�
     ifnull(t2.L_M_GROSS_AMT,0) as L_M_GROSS_AMT,                 --'����ë����
     ifnull(t2.L_M_SONSU_TIMES,0) as L_M_SONSU_TIMES,               --'�������Ѵ���
     ifnull(t2.Q_SALE_AMT,0) as Q_SALE_AMT,                    --'������������۶�
     ifnull(t2.Q_GROSS_AMT,0) as Q_GROSS_AMT,                   --'���������ë����
     ifnull(t2.Q_SONSU_TIMES,0) as Q_SONSU_TIMES,                 --'������������Ѵ���
     ifnull(t2.R_HALF_SALE_AMT,0) as R_HALF_SALE_AMT,               --'���������۶�
     ifnull(t2.R_HALF_GROSS_AMT,0) as R_HALF_GROSS_AMT,              --'������ë����
     ifnull(t2.R_HALF_SONSU_TIMES,0) as R_HALF_SONSU_TIMES,            --'���������Ѵ���
     ifnull(t2.R_YEAR_SALE_AMT,0) as R_YEAR_SALE_AMT,               --'��һ�����۶�
     ifnull(t2.R_YEAR_GROSS_AMT,0) as R_YEAR_GROSS_AMT,              --'��һ��ë����
     ifnull(t2.R_YEAR_SONSU_TIMES,0) as R_YEAR_SONSU_TIMES,            --'��һ�����Ѵ���
     ifnull(t2.R_LAST_YEAR_SALE_AMT,0) as R_LAST_YEAR_SALE_AMT,          --'��һ�����۶�
     ifnull(t2.R_LAST_YEAR_GROSS_AMT,0) as R_LAST_YEAR_GROSS_AMT,         --'��һ��ë����
     ifnull(t2.R_LAST_YEAR_SONSU_TIMES,0) as R_LAST_YEAR_SONSU_TIMES,       --'��һ�����Ѵ���
     ifnull(t2.R_ALL_SALE_AMT,0) as R_ALL_SALE_AMT,                --'�ۼ����۶�
     ifnull(t2.R_ALL_GROSS_AMT,0) as R_ALL_GROSS_AMT,               --'�ۼ�ë����
     ifnull(t2.R_ALL_SONSU_TIMES,0) as R_ALL_SONSU_TIMES,             --'�ۼ����Ѵ���
     ifnull(t4.NCD_TYPE,'N') AS NCD_TYPE,                      --������Ա��ʶ
     ifnull(t4.NCD_CNT,0) AS NCD_CNT,             --��һ������Ʒ�๺�����
     ifnull(t4.M_NCD_NCT,0) + ifnull(t2.CNT_R_ALL_NCD_CNT,0) as R_ALL_NCD_CNT, --�ۼ�����Ʒ�๺�����
     --t5.value_level as CLNT_CALU_LEVEL,--�ͻ���ֵ���� 
     null as CLNT_CALU_LEVEL,
     t3.FST_CUNSU_DATE,                --�״�����ʱ��
     t1.GNDR,                          --'�Ա�
     t1.BIRT_DATE,                     --'��������
     t1.BELONE_PVNC_NAME,              --'����ʡ��
     t1.BELONE_CITY_NAME,              --'������
     t1.BELONE_COUNTY_NAME,            --'��������
     t1.MEMB_CARD_STATE,               --'��Ա��״̬
     t1.MEMB_SOUR,                     --'��Ա��Դ
     t1.IS_WECHAT,                     --'�Ƿ��΢�Ż�Ա
     t1.IS_ALIPAY,                     --'�Ƿ��֧������Ա
     t1.IS_YF,                         --'�Ƿ��ע����ҩ��
     CASE WHEN ifnull(t2.R_ALL_SONSU_TIMES,0)=0 then '01' --δ���ѹ˿�
         WHEN t2.FF_SONSU_TIMES=1 then '02'         --�ͻ�Ծ��
         when t2.TWENTY_SONSU_TIMES>=1 and t2.TF_SONSU_TIMES>=1 and t2.FOF_SONSU_TIMES>=1 then '03'  --�ҳ�
         when t2.FF_SONSU_TIMES>=2 and (t2.TWENTY_SONSU_TIMES=0 or t2.TF_SONSU_TIMES=0 or t2.FOF_SONSU_TIMES=0) then '04' --�߻�Ծ��
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'55' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='110' then '05'  --�ͳ�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'110' and days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)<='180' then '06'  --�߳�˯��Ա
         when days_between(t3.LAST_TIME_CUNSU_DATE,:endTime)>'180' then '07'  --��ʧ��Ա
         else '00'                                                           
     end  as MEMB_LIFE_CYCLE,          --��Ա��������
   case when t3.FST_CUNSU_DATE is null and t1.OPEN_CARD_TIME is not null then t1.OPEN_CARD_TIME
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is  null then t3.FST_CUNSU_DATE
        when t3.FST_CUNSU_DATE is not null and t1.OPEN_CARD_TIME is not null then LEAST(OPEN_CARD_TIME,FST_CUNSU_DATE) end as OPEN_CARD_TIME,
     t1.OPEN_CARD_EMPE,                --'����Ա��
     t1.OPEN_CARD_PHMC_CODE,           --'�����ŵ����
     t1.OPEN_CARD_PHMC_NAME,           --'�����ŵ�����
     t1.OPEN_CARD_PHMC_STAR_BUSI,      --'�����ŵ꿪ҵ����
     t1.OPEN_CARD_PHMC_IS_MEDI_INSU,   --'�����ŵ��Ƿ�ҽ���ŵ�
     t1.OPEN_CARD_PHMC_PROR_ATTR,      --'�����ŵ��Ȩ����
     t1.OPEN_CARD_COMPANY_CODE,        --'������˾����
     t1.OPEN_CARD_COMPANY_NAME,        --'������˾����
     t1.OPEN_CARD_DEPRT_CODE,          --'�����Źܲ�����
     t1.OPEN_CARD_DEPRT_NAME,          --'�����Źܲ�����
     t1.OPEN_CARD_DIST_CODE,           --'����Ƭ������
     t1.OPEN_CARD_DIST_NAME,           --'����Ƭ������
     --t6.MAIN_CNSM_PHMC_CODE,           --'�������ŵ���� --ע��by ��ӱ�� 20191023 begin
     --t6.MAIN_CNSM_PHMC_NAME,           --'�������ŵ�����
     --t6.MAIN_CNSM_COMPANY_CODE,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_COMPANY_NAME,        --'�����ѹ�˾����
     --t6.MAIN_CNSM_DEPRT_CODE,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DEPRT_NAME,          --'�������Źܲ�����
     --t6.MAIN_CNSM_DIST_CODE,           --'������Ƭ������
     --t6.MAIN_CNSM_DIST_NAME,           --'������Ƭ������--ע��by ��ӱ�� 20191023 end
     t7.CREATE_TIME as NCD_CREA_TIME , --��������ʱ��     
     t8.LAST_BP_TIME,                  --���һ����Ѫѹʱ��
     t8.LAST_BS_TIME,                  --���һ�β�Ѫ��ʱ��
     t3.OFFLINE_FST_CNSM_PHMC_CODE,    --'�����״������ŵ����
     t3.OFFLINE_FST_CNSM_PHMC_NAME,    --'�����״������ŵ�����
     t3.OFFLINE_FST_CNSM_DATE,         --'�����״�����ʱ��
     IFNULL(t3.OFFLINE_FST_CNSM_AMT,0) AS OFFLINE_FST_CNSM_AMT, --'�����״����ѽ��
     t3.OFFLINE_LAST_CNSM_PHMC_CODE,   --'�������һ�������ŵ����
     t3.OFFLINE_LAST_CNSM_PHMC_NAME,   --'�������һ�������ŵ�����
     t3.OFFLINE_LAST_CNSM_DATE,         --'�������һ������ʱ��
     IFNULL(t3.OFFLINE_LAST_CNSM_AMT,0) AS OFFLINE_LAST_CNSM_AMT,          --'�������һ�����۽��
     IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) AS OFFLINE_Q_CNSM_AMT,           --����������������ѽ��
     case when IFNULL(t9.OFFLINE_Q_CNSM_AMT,0) =0 then 0 else t9.OFFLINE_Q_GROSS_AMT/t9.OFFLINE_Q_CNSM_AMT 
          end  as OFFLINE_Q_GROSS_RATE, --�������������ë����
     IFNULL(t9.OFFLINE_Q_CNSM_TIMES,0) AS OFFLINE_Q_CNSM_TIMES,          --����������������Ѵ���
     IFNULL(t9.OFFLINE_Y_CNSM_AMT,0) AS OFFLINE_Y_CNSM_AMT,           --�������1�����ѽ��
     case when IFNULL(t9.OFFLINE_Y_CNSM_AMT,0)=0 then 0 else t9.OFFLINE_Y_GROSS_AMT/t9.OFFLINE_Y_CNSM_AMT 
          end as OFFLINE_Y_GROSS_RATE, --�������1��ë����
     IFNULL(t9.OFFLINE_Y_CNSM_TIMES,0) AS OFFLINE_Y_CNSM_TIMES,           --�������1�����Ѵ���
	 ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_AMT,0) + ifnull(t9.MID_SALE_AMT_STSC,0) AS OFFLINE_TOTAL_CNSM_AMT ,  --�����ۼ����ѽ��
	 ifnull(t2.CNT_OFFLINE_TOTAL_GROS_AMT,0) + ifnull(t9.MID_GROSS_AMT_STSC,0) AS OFFLINE_TOTAL_GROS_AMT ,  --�����ۼ�����ë�����
     ifnull(t2.CNT_OFFLINE_TOTAL_CNSM_TIMES,0) + ifnull(t9.MID_SONSU_TIMES_STSC,0) AS OFFLINE_TOTAL_CNSM_TIMES ,  --�����ۼ����Ѵ���	  
     t4.R_YEAR_CNSM_GOODS_CODE,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_GOODS_NAME,         --'��һ�������ҩƷ����
     t4.R_YEAR_CNSM_CATE_CODE,          --'��һ�������Ʒ����루������
     t4.R_YEAR_CNSM_CATE_NAME,          --'��һ�������Ʒ�����ƣ�������
     t4.R_YEAR_TIME_PREFER,             --'��һ�����¹�ҩʱ���ƫ��
     ifnull(t4.R_YEAR_IS_MEDI,'0') as R_YEAR_IS_MEDI    --'��һ���Ƿ�ҽ��������
from  :var_base                       t1 
left join  :var_total_comsp           t2  on t1.member_id=t2.member_id
left join  :var_line_comsp            t3  on t1.member_id=t3.member_id
left join  :var_goods_payMode         t4  on t1.member_id=t4.member_id 
--left join  dm.memb_value_model_result t5  on t1.member_id=t5.member_id and t5.update_date = add_days(TO_DATE(:endTime, 'yyyymmdd'), 1)
--left join  :var_main_comsp            t6  on t1.member_id=t6.member_id --ע��by��ӱ�� 20191023
left join  DS_ZT.ZT_CHRONIC_BASELINE  t7  on T1.member_id=t7.customer_id
left join  :var_bp_bs                 t8  on t1.member_id=t8.customer_id 
left join  :var_agg                   t9  on t1.member_id=t9.member_id 
;


-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 

-------------------------------------��־����begin--------------------------------
var_sql_step  :=9;                     --SQL����
var_log_begin_time  :=current_timestamp;   --��ʼʱ��
var_step_comments :='�������ս��';                --����˵��

-------------------------------------��־����end----------------------------------
delete  from DM.FACT_MEMBER_CNT_INFO  where  DATA_DATE =   TO_DATE(:endTime,'yyyymmdd');
insert into DM.FACT_MEMBER_CNT_INFO
(
      DATA_DATE,--��������
      MEMBER_ID,--������Ա���
      OPEN_CARD_DAYS,--��������
      IS_OLD_MEMB,--�Ƿ��ϻ�Ա
      IS_ACTIV_MEMB,--�Ƿ��Ծ��Ա
      IS_EFFE_MEMB,--�Ƿ���Ч��Ա
      IS_ABNO_CARD,--�Ƿ��쳣��
      IS_NEGA_CARD,--�Ƿ񸺿�
      IS_EASY_MARKETING,--�Ƿ���Ӫ��
      LIFE_CYCLE_TYPE,--ʱ��Ƶ��������������
      ENTI_LIFE_CYCLE_TYPE,--����������������
      MEMBER_TYPE,--�ͻ���ʶ
      GNDR_AGE_TYPE,--�Ա���������
      UNIT_PRI,--�͵���
      UNIT_PRI_TYPE,--�͵�������
      GROS_MARGIN,--ë����
      GROS_MARGIN_TYPE,--ë��������
      LAST_TIME_CUNSU_DATE,--���һ������ʱ��
      R_MAX_SALE_INTERVAL,--ͳ��������Ѽ��
      R_MIN_SALE_INTERVAL,--ͳ����С���Ѽ��
      R_AVG_SALE_INTERVAL,--ͳ��ƽ�����Ѽ��
      NO_CONSU_TIME_LONG_MEMB_BEHAV,--δ����ʱ�䳤
      IS_ALI_PAY_MEMB_BEHAV,--�Ƿ�֧����֧��
      IS_WX_PAY_MEMB_BEHAV,--�Ƿ�΢��֧��
      IS_BANKCARD_PAY_MEMB_BEHAV,--�Ƿ�ˢ��֧��
      CLNT_CALU,--�ͻ���ֵ
      CLNT_CALU_HIS,--�ͻ���ʷ��ֵ
      M_SALE_AMT,--�������۶�
      M_GROSS_AMT,--����ë����
      M_SONSU_TIMES,--�������Ѵ���
      L_M_SALE_AMT,--�������۶�
      L_M_GROSS_AMT,--����ë����
      L_M_SONSU_TIMES,--�������Ѵ���
      Q_SALE_AMT,--������������۶�
      Q_GROSS_AMT,--���������ë����
      Q_SONSU_TIMES,--������������Ѵ���
      R_HALF_SALE_AMT,--���������۶�
      R_HALF_GROSS_AMT,--������ë����
      R_HALF_SONSU_TIMES,--���������Ѵ���
      R_YEAR_SALE_AMT,--��һ�����۶�
      R_YEAR_GROSS_AMT,--��һ��ë����
      R_YEAR_SONSU_TIMES,--��һ�����Ѵ���
      R_LAST_YEAR_SALE_AMT,--��һ�����۶�
      R_LAST_YEAR_GROSS_AMT,--��һ��ë����
      R_LAST_YEAR_SONSU_TIMES,--��һ�����Ѵ���
      R_ALL_SALE_AMT,--�ۼ����۶�
      R_ALL_GROSS_AMT,--�ۼ�ë����
      R_ALL_SONSU_TIMES,--�ۼ����Ѵ���
      NCD_TYPE,--������Ա��ʶ
      NCD_CNT,--��һ������Ʒ�๺�����
      R_ALL_NCD_CNT,--�ۼ�����Ʒ�๺�����
      CLNT_CALU_LEVEL,--�ͻ���ֵ����
      FST_CUNSU_DATE,--�״�����ʱ��
      GNDR,--�Ա�
      --BIRT_DATE,--�������� ע��by 20191023 begin
      --BELONE_PVNC_NAME,--����ʡ��
      --BELONE_CITY_NAME,--������
      --BELONE_COUNTY_NAME,--��������
      MEMB_CARD_STATE,--��Ա��״̬
      MEMB_SOUR,--��Ա��Դ
      IS_WECHAT,--�Ƿ��΢�Ż�Ա
      IS_ALIPAY,--�Ƿ��֧������Ա
      IS_YF,--�Ƿ��ע����ҩ��
      MEMB_LIFE_CYCLE,--��Ա��������
      OPEN_CARD_TIME,--����ʱ��
      --OPEN_CARD_EMPE,--����Ա��
      --OPEN_CARD_PHMC_CODE,--�����ŵ����
      --OPEN_CARD_PHMC_NAME,--�����ŵ�����
      --OPEN_CARD_PHMC_STAR_BUSI,--�����ŵ꿪ҵ����
      --OPEN_CARD_PHMC_IS_MEDI_INSU,--�����ŵ��Ƿ�ҽ���ŵ�
      --OPEN_CARD_PHMC_PROR_ATTR,--�����ŵ��Ȩ����
      --OPEN_CARD_COMPANY_CODE,--������˾����
      --OPEN_CARD_COMPANY_NAME,--������˾����
      --OPEN_CARD_DEPRT_CODE,--�����Źܲ�����
      --OPEN_CARD_DEPRT_NAME,--�����Źܲ�����
      --OPEN_CARD_DIST_CODE,--����Ƭ������
      --OPEN_CARD_DIST_NAME,--����Ƭ������
      --MAIN_CNSM_PHMC_CODE,--�������ŵ����
      --MAIN_CNSM_PHMC_NAME,--�������ŵ�����
      --MAIN_CNSM_COMPANY_CODE,--�����ѹ�˾����
      --MAIN_CNSM_COMPANY_NAME,--�����ѹ�˾����
      --MAIN_CNSM_DEPRT_CODE,--�������Źܲ�����
     -- MAIN_CNSM_DEPRT_NAME,--�������Źܲ�����
      --MAIN_CNSM_DIST_CODE,--������Ƭ������
      --MAIN_CNSM_DIST_NAME,--������Ƭ������ --�������� ע��by 20191023 end
      NCD_CREA_TIME,--��������ʱ��
      LAST_BP_TIME,--���һ����Ѫѹʱ��
      LAST_BS_TIME,--���һ�β�Ѫ��ʱ��
      OFFLINE_FST_CNSM_PHMC_CODE,--�����״������ŵ����
      OFFLINE_FST_CNSM_PHMC_NAME,--�����״������ŵ�����
      OFFLINE_FST_CNSM_DATE,--�����״�����ʱ��
      OFFLINE_FST_CNSM_AMT,--�����״����ѽ��
      OFFLINE_LAST_CNSM_PHMC_CODE,--�������һ�������ŵ����
      OFFLINE_LAST_CNSM_PHMC_NAME,--�������һ�������ŵ�����
      OFFLINE_LAST_CNSM_DATE,--�������һ������ʱ��
      OFFLINE_LAST_CNSM_AMT,--�������һ�����۽��
      OFFLINE_Q_CNSM_AMT,--����������������ѽ��
      OFFLINE_Q_GROSS_RATE,--�������������ë����
      OFFLINE_Q_CNSM_TIMES,--����������������Ѵ���
      OFFLINE_Y_CNSM_AMT,--�������һ�����ѽ��
      OFFLINE_Y_GROSS_RATE,--�������һ��ë����
      OFFLINE_Y_CNSM_TIMES,--�������һ�����Ѵ���
      OFFLINE_TOTAL_CNSM_AMT,--�����ۼ����ѽ��
      OFFLINE_TOTAL_GROS_AMT,--�����ۼ�ë����
      OFFLINE_TOTAL_CNSM_TIMES,--�����ۼ����Ѵ���
      R_YEAR_CNSM_GOODS_CODE,--��һ�������ҩƷ����
      R_YEAR_CNSM_GOODS_NAME,--��һ�������ҩƷ����
      R_YEAR_CNSM_CATE_CODE,--��һ�������Ʒ����루������
      R_YEAR_CNSM_CATE_NAME,--��һ�������Ʒ�����ƣ�������
      R_YEAR_TIME_PREFER,--��һ�����¹�ҩʱ���ƫ��
      R_YEAR_IS_MEDI,--��һ���Ƿ�ҽ��������
      LOAD_TIME      --����ʱ��
)
select 
     DATA_DATE,        --��������
	 MEMBER_ID,                 --��ԱID
     days_between(OPEN_CARD_TIME,:endTime) as OPEN_CARD_DAYS,--��������
     CASE WHEN days_between(OPEN_CARD_TIME,:endTime) > 365  then  '1'  else '0' END AS IS_OLD_MEMB,              --�Ƿ��ϻ�Ա
     IS_ACTIV_MEMB,	           --�Ƿ��Ծ��Ա
     CASE WHEN days_between(OPEN_CARD_TIME,TO_DATE(:endTime,'yyyymmdd')) >= 365 and R_YEAR_SONSU_TIMES >= 1 then '1'
          WHEN days_between(OPEN_CARD_TIME,TO_DATE(:endTime,'yyyymmdd')) < 365 and R_YEAR_SONSU_TIMES >= 2 then '1'
          ELSE '0' 
     END IS_EFFE_MEMB_STSC,        --�Ƿ���Ч��Ա 
	 IS_ABNO_CARD,              --�Ƿ��쳣��
	 IS_NEGA_CARD,        --�Ƿ񸺿�
	 IS_EASY_MARKETING,     --�Ƿ���Ӫ�� 
     LIFE_CYCLE_TYPE,      --ʱ��Ƶ��������������	 
     ENTI_LIFE_CYCLE_TYPE,    --���������������� 
     MEMBER_TYPE,              --�ͻ���ʶ(0302���¿�������)
     GNDR_AGE_TYPE,            --�Ա���������
     UNIT_PRI,        --�͵���
     UNIT_PRI_TYPE,            --�͵�������
     GROS_MARGIN,      --ë����
     GROS_MARGIN_TYPE,         --ë��������
     LAST_TIME_CUNSU_DATE,     --���һ������ʱ��
	 R_MAX_SALE_INTERVAL,       --ͳ��������Ѽ��
     R_MIN_SALE_INTERVAL,       --ͳ����С���Ѽ��
     R_AVG_SALE_INTERVAL,       --ƽ�����Ѽ�����
     NO_CONSU_TIME_LONG_MEMB_BEHAV, --δ����ʱ�䳤
     IS_ALI_PAY_MEMB_BEHAV,         --�Ƿ�֧����֧��
     IS_WX_PAY_MEMB_BEHAV,          --�Ƿ�΢��֧��
     IS_BANKCARD_PAY_MEMB_BEHAV,    --�Ƿ�ˢ��֧��
     CLNT_CALU,
     CLNT_CALU_HIS,
     M_SALE_AMT,                    --'�������۶�
     M_GROSS_AMT,                    --'����ë����
     M_SONSU_TIMES,                 --'�������Ѵ���
     L_M_SALE_AMT,                  --'�������۶�
     L_M_GROSS_AMT,                 --'����ë����
     L_M_SONSU_TIMES,               --'�������Ѵ���
     Q_SALE_AMT,                    --'������������۶�
     Q_GROSS_AMT,                   --'���������ë����
     Q_SONSU_TIMES,                 --'������������Ѵ���
     R_HALF_SALE_AMT,               --'���������۶�
     R_HALF_GROSS_AMT,              --'������ë����
     R_HALF_SONSU_TIMES,            --'���������Ѵ���
     R_YEAR_SALE_AMT,               --'��һ�����۶�
     R_YEAR_GROSS_AMT,              --'��һ��ë����
     R_YEAR_SONSU_TIMES,            --'��һ�����Ѵ���
     R_LAST_YEAR_SALE_AMT,          --'��һ�����۶�
     R_LAST_YEAR_GROSS_AMT,         --'��һ��ë����
     R_LAST_YEAR_SONSU_TIMES,       --'��һ�����Ѵ���
     R_ALL_SALE_AMT,                --'�ۼ����۶�
     R_ALL_GROSS_AMT,               --'�ۼ�ë����
     R_ALL_SONSU_TIMES,             --'�ۼ����Ѵ���
     NCD_TYPE,                      --������Ա��ʶ
     NCD_CNT,             --��һ������Ʒ�๺�����
     R_ALL_NCD_CNT, --�ۼ�����Ʒ�๺�����
     CLNT_CALU_LEVEL,
     FST_CUNSU_DATE,                --�״�����ʱ��
     GNDR,                          --'�Ա�
     --BIRT_DATE,                     --'��������  --�������� ע��by 20191023 begin
     --BELONE_PVNC_NAME,              --'����ʡ��
     --BELONE_CITY_NAME,              --'������
     --BELONE_COUNTY_NAME,            --'��������
     MEMB_CARD_STATE,               --'��Ա��״̬
     MEMB_SOUR,                     --'��Ա��Դ
     IS_WECHAT,                     --'�Ƿ��΢�Ż�Ա
     IS_ALIPAY,                     --'�Ƿ��֧������Ա
     IS_YF,                         --'�Ƿ��ע����ҩ��
     MEMB_LIFE_CYCLE,          		--��Ա��������
     OPEN_CARD_TIME,                --'����ʱ��
    -- OPEN_CARD_EMPE,                --'����Ա�� 
     --OPEN_CARD_PHMC_CODE,           --'�����ŵ����
     --OPEN_CARD_PHMC_NAME,           --'�����ŵ�����
     --OPEN_CARD_PHMC_STAR_BUSI,      --'�����ŵ꿪ҵ����
     --OPEN_CARD_PHMC_IS_MEDI_INSU,   --'�����ŵ��Ƿ�ҽ���ŵ�
     --OPEN_CARD_PHMC_PROR_ATTR,      --'�����ŵ��Ȩ����
     --OPEN_CARD_COMPANY_CODE,        --'������˾����
     --OPEN_CARD_COMPANY_NAME,        --'������˾����
     --OPEN_CARD_DEPRT_CODE,          --'�����Źܲ�����
     --OPEN_CARD_DEPRT_NAME,          --'�����Źܲ�����
     --OPEN_CARD_DIST_CODE,           --'����Ƭ������
     --OPEN_CARD_DIST_NAME,           --'����Ƭ������
     --MAIN_CNSM_PHMC_CODE,           --'�������ŵ����
     --MAIN_CNSM_PHMC_NAME,           --'�������ŵ�����
     --MAIN_CNSM_COMPANY_CODE,        --'�����ѹ�˾����
     --MAIN_CNSM_COMPANY_NAME,        --'�����ѹ�˾����
     --MAIN_CNSM_DEPRT_CODE,          --'�������Źܲ�����
     --MAIN_CNSM_DEPRT_NAME,          --'�������Źܲ�����
     --MAIN_CNSM_DIST_CODE,           --'������Ƭ������
     --MAIN_CNSM_DIST_NAME,           --'������Ƭ������ --�������� ע��by 20191023 end
     NCD_CREA_TIME ,                --��������ʱ��     
     LAST_BP_TIME,                  --���һ����Ѫѹʱ��
     LAST_BS_TIME,                  --���һ�β�Ѫ��ʱ��
     OFFLINE_FST_CNSM_PHMC_CODE,    --'�����״������ŵ����
     OFFLINE_FST_CNSM_PHMC_NAME,    --'�����״������ŵ�����
     OFFLINE_FST_CNSM_DATE,         --'�����״�����ʱ��
     OFFLINE_FST_CNSM_AMT, --'�����״����ѽ��
     OFFLINE_LAST_CNSM_PHMC_CODE,   --'�������һ�������ŵ����
     OFFLINE_LAST_CNSM_PHMC_NAME,   --'�������һ�������ŵ�����
     OFFLINE_LAST_CNSM_DATE,         --'�������һ������ʱ��
     OFFLINE_LAST_CNSM_AMT,          --'�������һ�����۽��
     OFFLINE_Q_CNSM_AMT,           --����������������ѽ��
     OFFLINE_Q_GROSS_RATE, --�������������ë����
     OFFLINE_Q_CNSM_TIMES,          --����������������Ѵ���
     OFFLINE_Y_CNSM_AMT,           --�������1�����ѽ��
     OFFLINE_Y_GROSS_RATE, --�������1��ë����
     OFFLINE_Y_CNSM_TIMES,           --�������1�����Ѵ���
	 OFFLINE_TOTAL_CNSM_AMT ,  --�����ۼ����ѽ��
	 OFFLINE_TOTAL_GROS_AMT ,  --�����ۼ�����ë�����
     OFFLINE_TOTAL_CNSM_TIMES ,  --�����ۼ����Ѵ���	  
     R_YEAR_CNSM_GOODS_CODE,         --'��һ�������ҩƷ����
     R_YEAR_CNSM_GOODS_NAME,         --'��һ�������ҩƷ����
     R_YEAR_CNSM_CATE_CODE,          --'��һ�������Ʒ����루������
     R_YEAR_CNSM_CATE_NAME,          --'��һ�������Ʒ�����ƣ�������
     R_YEAR_TIME_PREFER,             --'��һ�����¹�ҩʱ���ƫ��
     R_YEAR_IS_MEDI,    --'��һ���Ƿ�ҽ��������
     current_timestamp  AS LOAD_TIME                --����ʱ��
from  :var_total
;

-------------------------------------ִ��SQLд����־��ʼ---------------------------			 
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
-------------------------------------ִ��SQLд����־����--------------------------- 
commit;
end;
