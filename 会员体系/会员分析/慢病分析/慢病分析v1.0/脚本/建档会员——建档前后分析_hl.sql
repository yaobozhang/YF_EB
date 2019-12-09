
drop table EXT_TMP.HY_SALE;				--ɾ��������ϸ��
CREATE TABLE EXT_TMP.HY_SALE AS (		--����������Ա��������ϸ����Ϊ������Ա����ǰ��X���µ���������Ļ���
	select
			p.MEMB_CODE					--��Ա����	
			,p.STSC_DATE				--��������
			,p.phmc_code				--�ŵ���
			,sum(SALE_AMT) SALE_AMT		--���ѽ��
			,SUM(GROS_PROF_AMT) AS SALE_GROS		--����ë��
			from DW.FACT_SALE_ORDR_DETL p
		   left join dw.DIM_GOODS_H g on g.goods_sid=p.goods_sid
		   where p.stsc_date >= '2016-10-1'		--ʱ���ȡ2016��10��
		   and  p.stsc_date < '2019-7-1' 		--��2019��6��
		   and "ORDR_CATE_CODE"<>'3'			--�޳���������Ϊ3��4�Ķ���
		   and "ORDR_CATE_CODE"<>'4'		
		   and "PROD_CATE_LEV1_CODE"<>'Y85'		--�޳���Ʒ
		   and "PROD_CATE_LEV1_CODE"<>'Y86'		--�޳���������Ʒ
		   and g."GOODS_CODE" <> '8000875'		
		   and g."GOODS_CODE" <> '8000874' 
		   and p.member_id is not null			--ѡȡ��Ա
		   and exists(							--ֻѡȡ������Ա��������ϸ
				select 1 from
				(
					select
					card_code
					,min(create_time) mb_date
					from "DS_ZT"."ZT_CHRONIC_BASELINE" 
					group by
					card_code
				) t2
				where p.MEMB_CODE=t2.card_code
		   )
		   group by p.MEMB_CODE
			,p.STSC_DATE
			,p.phmc_code			
)


--��������
with t0_1 as (							--��������ǰ��12���£�����ƥ�佨����Ա����ǰ��X���µ����ѵ�������	
select
Xth_mon
from (
select Row_Number() OVER(ORDER BY code desc)-13 Xth_mon
 from "DW"."BI_TEMP_COUPON_ALL" limit 25
 )
 where Xth_mon<> 0 
)
,
--STEP0.2:������Ա��Ϣ
t0_2 as (								--��ȡ������Ա�Ŀ��źͽ���ʱ�䣬���ڼ��㽨��ǰ��X����
select
card_code
,to_date(min(create_time)) as mb_date
from "DS_ZT"."ZT_CHRONIC_BASELINE"
where card_code is not null
group by
card_code
)
,
--����������Ա����ǰ��12���µ�ģ��
t0_3 as (
select
card_code
,Xth_mon
from t0_2 
inner join t0_1 on 1=1
)
,

--STEP1:������Ա�������
-- ������Ա������ϸ
t1_1 as (
   select 
		 d.memb_code as card_code 	--������Ա����
		,to_char(stsc_date,'yyyymmdd') stsc_date	--��������
		,count(1) over(partition by memb_code, stsc_date ) xf_num --ÿ��������Ա�����Ѵ���
		,SALE_AMT		--ÿ��������Աÿ������ѽ��
		,SALE_GROS			--ÿ��������Աÿ��Ĺ���ë����
   from EXT_TMP.HY_SALE d 
   where d.memb_code is not null
  )
 ,
 --������Ա���ѻ��ܡ�������
 t1 as (
  select
	 card_code					--������Ա����
	 ,stsc_date 				--��������
	 ,'1' as dd_type			--���� 1��������ΪΪ����
	 ,max(xf_num)	dd_num		--�������
	 ,max(xf_num)	xf_num		--���Ѵ���
	,sum(SALE_AMT) SALE_AMT		--���ѽ��
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
	select  card_code		--������Ա����
		 ,stsc_date			--�������
		,count(1) over(partition by card_code, stsc_date ) dd_num --ÿ��������Աÿ�쵽�����
		,xf_num				--���Ѵ���
		,SALE_AMT			--���ѽ��
		,SALE_GROS			--����ë��
	from
	(
		select	
			t.card_code			--������Ա	
			 ,to_char(t4.RECORD_date,'yyyymmdd') stsc_date		--�������
			 ,0 AS SALE_AMT		--�������ѽ����ڵ�������ļ���	
			 ,0 AS SALE_GROS	--��������ë�������ڵ�������ļ���
			,0 as xf_num		--�������Ѵ��������ڵ�������ļ���
		 from 
		 "DS_ZT"."ZT_CHRONIC_BASELINE" t
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORDER" t3 on t.customer_id=t3.customer_id 
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORD" t4 on t4.recorder_id=t3.id 
		 where t4.IS_DELETE='1'	
		 and t4.record_from = 'MB_ST'		--���ŵ�
		 and t.card_code is not null		--������Ա���Ų�Ϊ��
	 )
 --limit 1000
 )
 
,
--������Ա������
t2 as (
  select
	 card_code			--������Ա����
	 ,stsc_date 		--�������
	 ,'2' as dd_type	--���� 2��������ΪΪ��Ѫ��Ѫѹ
	 ,max(dd_num)	dd_num		--�������
	 ,max(xf_num)	xf_num		--���Ѵ���
	,sum(SALE_AMT) SALE_AMT		--���ѽ��
	,sum(SALE_GROS) SALE_GROS	--����ë��
 from t2_1
 group by 
	card_code 	--һ����һ�������
	,stsc_date	
) 
,


--STEP3:������Ա�������		�ϲ���������ͼ����������ڻ��ܵ������
t3_1 as (
select 	 card_code
	 ,stsc_date 
	 ,dd_type		--���� 1
	 ,	dd_num		--�������
	 ,	xf_num	--���Ѵ���
	, SALE_AMT	--���ѽ��
	, SALE_GROS	--����ë��
from t1 
union all
select 	card_code
	 ,stsc_date 
	 ,dd_type		--���� 2
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
	 ,case when flag>1 then '3' else dd_type end as dd_type		--���� 3��������Ϊ�����������м��
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
--�����õ�����ʱ��			--���ڼ��㽨��ǰ��X���µĵ������
t4 as(
	select
		card_code
		 ,Xth_mon 
		 ,sum(dd_num) as dd_num	--�������
		 ,sum(xf_num) as xf_num	--���Ѵ���
		, sum(SALE_AMT) as SALE_AMT 	--���ѽ��
		, sum(SALE_GROS) as SALE_GROS	--����ë��
	from
	(
		select 	 t3.card_code
			 ,t3.stsc_date 
			 ,t3.dd_type		
			 ,	t3.dd_num		--�������
			 ,	t3.xf_num	--���Ѵ���
			, t3.SALE_AMT	--���ѽ��
			, t3.SALE_GROS	--����ë��
			,t4.mb_date	--����ʱ��
			,case when t3.stsc_date>=t4.mb_date then months_between(t4.mb_date,t3.stsc_date)+1		--������
			else months_between(t4.mb_date,t3.stsc_date) end Xth_mon						--����ǰ
		from t3
		left join t0_2 t4
		on t3.card_code=t4.card_code
	)
	group by card_code
		 ,Xth_mon 
	
)
,
--�����õ�ÿ����ǰ��12�±�����ϸ
t5 as(
	select
	t1.card_code	--����
	,t1.Xth_mon	--����ǰ��
	,ifnull(t4.dd_num,0)  as dd_num	--�������
	,ifnull(t4.xf_num,0)  as xf_num	--���Ѵ���
	,ifnull(t4.SALE_AMT,0)  as SALE_AMT 	--���ѽ��
	,ifnull(t4.SALE_GROS,0)  as SALE_GROS	--����ë��
	from t0_3 t1
	left join t4
	on t1.card_code=t4.card_code
	and t1.Xth_mon=t4.Xth_mon
	
)
,
--�����õ�ÿ���½����ŵ�
t6_1 as (
		select
	t1.card_code	--����
	,t1.Xth_mon	--����ǰ��
	,t1.dd_num	--�������
	,t1.xf_num	--���Ѵ���
	,t1.SALE_AMT 	--���ѽ��
	,t1.SALE_GROS	--����ë��
	,t2.store_code
	from t5 t1
	inner join "DS_ZT"."ZT_CHRONIC_BASELINE" t2 on t1.card_code = t2.card_code
	where t2.store_code is not null
)
,
--�ŵ���ˣ��õ������ŵ����ݼ�����
t6 as(
	select
	t1.card_code	--����
	,t1.Xth_mon	--����ǰ��
	,t1.dd_num	--�������
	,t1.xf_num	--���Ѵ���
	,t1.SALE_AMT 	--���ѽ��
	,t1.SALE_GROS	--����ë��
	,t1.store_code		--�����ŵ�
	,t2."phmc_type"		--�ŵ�����
	from t6_1 t1
	inner join "EXT_TMP"."phmc_mb_all" t2
	on t1.store_code=t2."phmc_code"
	where t2."phmc_code" is not null
)

select 
	--"phmc_type"
	--,store_code
	Xth_mon
	,sum(case when dd_num >0 then 1 else 0 end) as memb_dd_num
	,sum(case when xf_num >0 then 1 else 0 end) as memb_xf_num
	,sum(dd_num)_dd_num		--����Ƶ��
	,sum(xf_num)xf_num		--����Ƶ��
	,sum(SALE_AMT)SALE_AMT	--���۽��
	,sum(SALE_GROS) SALE_GROS	
from t6
group by --"phmc_type",store_code,
Xth_mon
order by --"phmc_type",store_code,
Xth_mon
limit 4000
,
--step7:��ۺϣ��õ�ÿ��ά��������
--�ȿ��ܵĽ���ǰ������
t7_1 as (
	select 
	"phmc_type"
	,Xth_mon
		,sum(dd_num)/sum(case when dd_num >0 then 1 else 0 end) as memb_avg_dd_num		--�˾�����Ƶ��
		,sum(xf_num)/sum(case when xf_num >0 then 1 else 0 end) as memb_avg_xf_num		--�˾�����Ƶ��
		,sum(SALE_AMT)/sum(case when SALE_AMT >0 then 1 else 0 end) as memb_avg_SALE_AMT	--�˾����۽��
		,sum(SALE_GROS)/sum(case when SALE_AMT >0 then 1 else 0 end) as memb_avg_SALE_GROS	--�˾�����ë��
	from t6
	group by "phmc_type",Xth_mon
	
)

select * from t7_1


