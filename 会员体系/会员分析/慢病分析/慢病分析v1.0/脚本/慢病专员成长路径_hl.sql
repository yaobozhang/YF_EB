--�����ݣ��õ�ÿ�����������
with t0_1_1 as (
	select add_days(to_date('2017-10-01','yyyy-mm-dd'),Row_Number() OVER(ORDER BY code desc)-1) dates
	from "DW"."BI_TEMP_COUPON_ALL" limit 638
)
,
 --STEP0_1:����רԱ�ɳ�ʱ��
--����רԱ�ɳ�����ʱ�䵽��
t0_2_1 as (
	 select
	 "worker_code"		--����רԱ
	 ,"start_date"		--��ְʱ��
	 ,dates			--����
	 ,t.CARD_CODE	--������Ա
	 ,to_date(t.create_time) mb_date
	 from "EXT_TMP"."mb_worker" p
	 inner join t0_1_1 on to_char(t0_1_1.dates,'yyyymmdd') >= to_char(p."start_date",'yyyymmdd')
	 inner join "DS_ZT"."ZT_CHRONIC_BASELINE" t on p."worker_code" = t.creator
	 where t.CARD_CODE is not null
 )
 ,
 
--STEP1:������Ա�������
-- ������Ա������ϸ
t1_1 as (
   select 
		 d.memb_code as card_code 	
		,to_char(stsc_date,'yyyymmdd') stsc_date	--��������
		,count(1) over(partition by memb_code, stsc_date ) xf_num --���Ѵ���
		,SALE_AMT		--ÿ�����ѽ��
		,SALE_GROS			--ÿ�칱��ë����
   from EXT_TMP.HY_SALE d 
   where d.memb_code is not null
 )
 ,
 --������Ա���ѻ��ܡ�������
 t1 as (
  select
	 card_code
	 ,stsc_date 
	 ,'1' as dd_type		--���� 1��������
	 ,max(xf_num)	dd_num		--�������
	 ,max(xf_num)	xf_num	--���Ѵ���
	,sum(SALE_AMT) SALE_AMT	--���ѽ��
	,sum(SALE_GROS) SALE_GROS	--����ë��
 from t1_1
 group by 
	card_code 	--һ����һ���������
	,stsc_date	
 )
 ,
 --STEP2:������Ա�����󵽵����
--������Ա������������
t2_1 as (
	select  card_code		--������Ա
		 ,stsc_date		--����
		,count(1) over(partition by card_code, stsc_date ) dd_num --�������
		,xf_num				--���Ѵ���
		,SALE_AMT			--���ѽ��
		,SALE_GROS			--����ë��
	from
	(
		select	
			t.card_code	--������Ա	
			 ,to_char(t4.RECORD_date,'yyyymmdd') stsc_date		--�������
			 ,0 AS SALE_AMT			
			 ,0 AS SALE_GROS
			,0 as xf_num
		 from 
		 "DS_ZT"."ZT_CHRONIC_BASELINE" t
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORDER" t3 on t.customer_id=t3.customer_id 
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORD" t4 on t4.recorder_id=t3.id 
		 where t4.IS_DELETE='1'
		 and t4.record_from = 'MB_ST'
		 and t.card_code is not null
	 )
 --limit 1000
 )
 
,
--������Ա������
t2 as (
  select
	 card_code
	 ,stsc_date 
	 ,'2' as dd_type		--���� 1��������
	 ,max(dd_num)	dd_num		--�������
	 ,max(xf_num)	xf_num	--���Ѵ���
	,sum(SALE_AMT) SALE_AMT	--���ѽ��
	,sum(SALE_GROS) SALE_GROS	--����ë��
 from t2_1
 group by 
	card_code 	--һ����һ���������
	,stsc_date	
) 


,

--STEP3:������Ա�������
t3_1 as (
select 	 card_code
	 ,stsc_date 
	 ,dd_type		--���� 1��������
	 ,	dd_num		--�������
	 ,	xf_num	--���Ѵ���
	, SALE_AMT	--���ѽ��
	, SALE_GROS	--����ë��
from t1 
union all
select 	card_code
	 ,stsc_date 
	 ,dd_type		--���� 1��������
	 ,	dd_num		--�������
	 ,	xf_num	--���Ѵ���
	, SALE_AMT	--���ѽ��
	, SALE_GROS	--����ë��
from t2
)
,
t3_2 as
(
	select 	card_code
	 ,stsc_date 
	 ,count(1) over(partition by card_code, stsc_date ) flag1	--�ж�flag
	,dd_type
	 ,dd_num		--�������
	 ,xf_num	--���Ѵ���
	,SALE_AMT	--���ѽ��
	,SALE_GROS	--����ë��
	from t3_1
)
,
--�ۺϵõ����Ѽ����ϲ�����
t3 as (
	select 	card_code
	 ,stsc_date 
	 ,case when flag>1 then '3' else dd_type end as dd_type		--���� 1��������
	 ,dd_num		--�������
	 ,xf_num	--���Ѵ���
	,SALE_AMT	--���ѽ��
	,SALE_GROS	--����ë��
	from
	(
		select 	card_code
		 ,stsc_date
		 ,max(flag1) as flag
		 ,max(dd_type) as dd_type
		 --,case when count(1)>1 then '3' else dd_type end as dd_type		--���� 1��������
		 ,max(dd_num) as dd_num		--�������
		 ,max(xf_num) as xf_num	--���Ѵ���
		, sum(SALE_AMT) as SALE_AMT	--���ѽ��
		, sum(SALE_GROS) as SALE_GROS	--����ë��
		from t3_2
		group by card_code,stsc_date
	)

)

,
--�ۺ�
t4 as(
select
 t1."worker_code"		--����רԱ
 ,t1."start_date"		--��ְʱ��
 ,t1.dates			--����
,t1.mb_date
 ,t1.CARD_CODE	--������Ա
,t2.dd_type		--���� 1��������
,t2.dd_num		--�������
,t2.xf_num	--���Ѵ���
,t2.SALE_AMT	--���ѽ��
,t2.SALE_GROS	--����ë��
from t0_2_1 t1
left join t3 t2 on t1.CARD_CODE = t2.CARD_CODE and to_char(t1.dates,'yyyymmdd') = t2.stsc_date
where t1.dates>=t1.mb_date		--ͳ���������ڽ���ʱ��
and t1.mb_date >= "start_date"	--��ְʱ�����ڽ���ʱ��
)
--select * from t4 order by "worker_code" asc limit 100
,
--ͳ��ÿ������רԱ�����ÿ�������
t5 as (
select
 "worker_code"	as worker_code	--����רԱ
 ,to_char(dates,'yyyymm') as mon			--����
,count(distinct case when to_char(mb_date,'yyyymm') = to_char(dates,'yyyymm') then CARD_CODE end) jd_menb
,count(distinct case when dd_num >0 then CARD_CODE end) as memb_dd_num
,count(distinct case when xf_num >0 then CARD_CODE end) as memb_xf_num
-- ,count(CARD_CODE	--������Ա
,sum(dd_num) as dd_num		--�������
,sum(xf_num) as xf_num	--���Ѵ���
,sum(SALE_AMT) as SALE_AMT	--���ѽ��
--,sum(SALE_GROS)	 as SALE_GROS	--����ë��
from t4
group by 
 "worker_code"		--����רԱ
 ,to_char(dates,'yyyymm')
)

,

--ͳ��
t6_1 as (
select
worker_code
,mon
,rank() OVER (partition by worker_code ORDER BY mon) Xth_mon
,sum(jd_menb) OVER (partition by worker_code ORDER BY mon) sum_jd_people_num	
,jd_menb
,memb_dd_num
,memb_xf_num
,ifnull(dd_num,0) dd_num
,ifnull(xf_num,0) xf_num
,ifnull(SALE_AMT,0) SALE_AMT
from t5
)
,
t6 as (
select
t6_1.worker_code
,mon
,Xth_mon
,sum_jd_people_num
,jd_menb
--,case when memb_xf_num <> 0 then SALE_AMT/memb_xf_num else 0 end price
,memb_dd_num 
,memb_xf_num
,dd_num
,xf_num
,SALE_AMT
from t6_1
where jd_menb<>0
order by 
worker_code
,mon
)
,


t7_1 as (
	select 
	worker_code,
	min(mon) mon,
	case --when SALE_AMT=0 then 0
	--20k
	when SALE_AMT>0 and SALE_AMT<20000 then 1
	when SALE_AMT>=20000 and SALE_AMT<40000 then 2
	when SALE_AMT>=40000 and SALE_AMT<60000 then 3
	when SALE_AMT>=60000 and SALE_AMT<80000 then 4
	when SALE_AMT>=80000 and SALE_AMT<100000 then 5
	when SALE_AMT>=100000 and SALE_AMT<120000 then 6
	when SALE_AMT>=120000 and SALE_AMT<140000 then 7
	when SALE_AMT>=140000 and SALE_AMT<160000 then 8
	when SALE_AMT>=160000 and SALE_AMT<180000 then 9
	when SALE_AMT>=180000 and SALE_AMT<200000 then 10
	when SALE_AMT>=200000 then 11 end as LV 
	from t6
	group by worker_code,
	case
	when SALE_AMT>0 and SALE_AMT<20000 then 1
	when SALE_AMT>=20000 and SALE_AMT<40000 then 2
	when SALE_AMT>=40000 and SALE_AMT<60000 then 3
	when SALE_AMT>=60000 and SALE_AMT<80000 then 4
	when SALE_AMT>=80000 and SALE_AMT<100000 then 5
	when SALE_AMT>=100000 and SALE_AMT<120000 then 6
	when SALE_AMT>=120000 and SALE_AMT<140000 then 7
	when SALE_AMT>=140000 and SALE_AMT<160000 then 8
	when SALE_AMT>=160000 and SALE_AMT<180000 then 9
	when SALE_AMT>=180000 and SALE_AMT<200000 then 10
	when SALE_AMT>=200000 then 11 end
	order by
	worker_code,
	mon	
)
--select * from t7_1 order by worker_code asc,mon asc limit 100
,
t7_2 as (
	select worker_code,
		mon,
		lv,
		row_number() over(partition by worker_code order by mon ASC) as rn 
	from t7_1
		
)
,
t7_3 as(
	select t1.worker_code
		,t1.mon
		,t1.lv
		,t2.lv as lv_2
	from t7_2 t1
	left join t7_2 t2
	on t1.worker_code=t2.worker_code
	and t1.rn>t2.rn
)
,
t7 as(
	select t1.worker_code
		,t1.mon
		,t1.lv
	from
	(
	select t1.worker_code
		,t1.mon
		,t1.lv
		,max(lv_2) as lv_flag
	from t7_3 t1
	group by t1.worker_code
		,t1.mon
		,t1.lv
	)t1 where lv>lv_flag or lv=1
)
--select * from t7 order by worker_code asc,mon asc limit 1000
,
--�����õ���ֵ
t8 as(
	select T1.worker_code
			,t1.mon mon1
			,t2.mon mon2
		,t1.LV
		,t2.LV as lv_2
		,case when t2.LV IS NULL THEN 0 
		else MONTHS_BETWEEN(to_date(t2.mon,'yyyymm'),to_date(t1.mon,'yyyymm')) 
		 end as mon_diff
	from 
	(
		select worker_code,
			mon,
			LV,
			LV-1 AS LV_BEFORE
		from t7
		--WHERE lv>=1
	) t1 
	left join t7 t2
	on t1.worker_code=t2.worker_code
	and t1.LV_BEFORE=t2.LV
)
--select * from t8 order by worker_code asc,mon1 asc limit 1000
,

--select * from t8 where worker_code='00119115' order by worker_code,mon1 asc limit 1000
--���������йؼ�����ȡ���õ�����ƽ��ʱ��
t9 AS(
	SELECT LV
		,lv_2
		,AVG(mon_diff)
	FROM
	(
		SELECT worker_code
			,mon1
			,mon2
			,LV
			,lv_2
			,mon_diff
		FROM T8
		WHERE mon2 IS NOT NULL
	)
	GROUP BY LV,lv_2
)
--SELECT * FROM t9
,
--�õ�ÿ������רԱ�ĳɳ�·��
t10 as (
select
t1.worker_code
,t1.mon
,t2.Xth_mon
,t1.LV
,sum_jd_people_num
,jd_menb
--,case when memb_xf_num <> 0 then SALE_AMT/memb_xf_num else 0 end price
,memb_dd_num 
,memb_xf_num
,dd_num
,xf_num
,SALE_AMT
from t7 t1 
inner join t6 t2 on t1.worker_code = t2.worker_code and t1.mon = t2.mon
order by
t1.worker_code
,t1.mon
,t1.LV
)
,
--�õ�ÿ����������רԱ����������ƽ��ֵ
t11 as
(
	select lv
		,count(distinct worker_code) as worker_num
		,avg(sum_jd_people_num)		--������Ա��
		,avg(memb_dd_num)	--�»�Ծ��Ա��
		,avg(memb_xf_num)		--�����ѻ�Ա��
		,SUM(SALE_AMT)/	SUM(memb_xf_num)	--�˾������ѽ��
		,sum(xf_num)/sum(memb_xf_num)		--�˾�������Ƶ��
		,sum(dd_num)/sum(memb_dd_num)		--�˾��µ���Ƶ��
	from t10
	group by lv
)

select * from t11 limit 1000
