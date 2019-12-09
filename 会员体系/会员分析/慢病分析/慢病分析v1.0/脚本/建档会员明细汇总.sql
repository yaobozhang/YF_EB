

--������
with t0_1 as (
	select add_days(to_date('2017-10-01','yyyy-mm-dd'),Row_Number() OVER(ORDER BY code desc)-1) dates
	from "DW"."BI_TEMP_COUPON_ALL" limit 638
	)
,
--STEP0:������Ա��Ϣ
 t0_2 as (
select
card_code
,to_date(min(create_time)) as mb_date
from "DS_ZT"."ZT_CHRONIC_BASELINE"
where card_code is not null
group by
card_code
)
,

t0 as (
select
card_code
,mb_date
,dates
from t0_2 t1
inner join t0_1 t2 on 1=1
)

select
to_char(dates,'yyyymm') mon 
count(distinct )

--select dates from t0 limit 100
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
--select stsc_date from t3  limit 100
,

--���ܡ�������
t4 as (
		select
		t1.card_code	--��Ա����
		,t1.mb_date	--����ʱ��
		,t1.dates	--ͳ������
		,t2.dd_num		--�������
		,t2.xf_num	--���Ѵ���
		,t2.SALE_AMT	--���ѽ��
		--,SALE_GROS	--����ë��
		from t0 t1
		left join t3 t2 on t1.card_code = t2.card_code and t1.dates = t2.stsc_date
		where t1.dates>=t1.mb_date		--ͳ���������ڽ���ʱ��
)
select count(distinct card_code) from t4
,

--ͳ�ƽ�����Աÿ�������
t5 as (
select
 to_char(dates,'yyyymm') as mon			--����
,rank() OVER (ORDER BY to_char(dates,'yyyymm')) Xth_mon
,count(distinct case when to_char(mb_date,'yyyymm') = to_char(dates,'yyyymm') then CARD_CODE end) jd_memb --��������
,count(distinct case when dd_num >0 then CARD_CODE end) as memb_dd_num		--��������
,count(distinct case when xf_num >0 then CARD_CODE end) as memb_xf_num		--��������
,sum(dd_num) as dd_times		--�������
,sum(xf_num) as xf_times	--���Ѵ���
,sum(SALE_AMT) as SALE_AMT	--���ѽ��
--,sum(SALE_GROS)	 as SALE_GROS	--����ë��
from t4
group by 
 to_char(dates,'yyyymm')
)
,

t6 as (
select
mon
,Xth_mon
--,sum(jd_memb) OVER (partition by Xth_mon) sum_jd_people_num	
,jd_memb
,memb_dd_num
,memb_xf_num
,ifnull(dd_times,0) dd_times
,ifnull(xf_times,0) xf_times
,ifnull(SALE_AMT,0) SALE_AMT
from t5
)
,
t7 as (
	select
	t1.mon
	,t1.Xth_mon
	,t2.sum_jd_memb
	,t1.jd_memb
	,t1.memb_dd_num
	,t1.memb_xf_num
	,t1.dd_times
	,t1.xf_times
	,t1.SALE_AMT
	from t6 t1
	inner join (
		select 
		t.mon
		,t.Xth_mon
		,sum(t1.jd_memb) sum_jd_memb
		--,sum(t1.jd_memb) jd_memb
		from t6 t
		left join t6 t1 on t.Xth_mon>=t1.Xth_mon
		group by	
		t.mon
		,t.Xth_mon
		) t2 on t1.mon = t2.mon and t1.Xth_mon = t2.Xth_mon
		
)

		
		
select
	t1.mon  		--�·�
	,t1.Xth_mon		--��X��
	,t1.sum_jd_memb	--�ۼƽ�������
	,t1.jd_memb		--��������
	,t1.memb_dd_num	--��������
	,t1.memb_xf_num	--��������
	,t1.dd_times	--�������
	,t1.xf_times	--���Ѵ���
	,t1.SALE_AMT	--���ѽ��
	,t1.dd_times/t1.memb_dd_num		avg_dd_times--�˾��������
	,t1.xf_times/t1.memb_xf_num		avg_xf_times--�˾����Ѵ���
	,t1.SALE_AMT/t1.memb_xf_num		avg_SALE_AMT--�˾����ѽ��
from t7 t1
order by 
mon