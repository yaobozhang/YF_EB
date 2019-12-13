--�ô���Ϊ��Ա�ּ��������ݷ�������
--���빱���ߣ�Ҧ����
--ʱ�䣺20190609


-----------------------------------���ȣ��õ�20180601��Ա��ʼ������---------------------------
with t1_1 as
(
	select member_id
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2018-06-01',-1)
	and s.stsc_date<'2018-06-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
	/*
	and not exists		--�ŵ���ˣ�1��������ڷ������ڣ�2���Ϻ�ҽ����3����ͣ
	(
		select 1 from dw.DIM_PHMC g1
		where g1.PHMC_CODE = s.PHMC_CODE 
		and 
		(	--�Ϻ���˾ҽ������߿���ʱ�����20190501�����йص�ʱ����޳�
			g1.STAR_BUSI_TIME >= '20190601' 
			or (g1.PHMC_S_NAME like '%ҽ��%' and g1.ADMS_ORG_CODE = '1001' )
			or CLOSE_DATE is not null
			or PROP_ATTR in ('Z02','Z07')		--�չ�
			or company_code='4000'			--����
		 )
	)
	*/
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
)
,

--���л�Ա����
t1_2 as
(
	select t1.member_id	
		,case when t2.SALE_AMOUNT is null then 0 else floor(t2.SALE_AMOUNT) end as gro_point	--�ɳ�ֵ
		,case when t1.create_time<'2017-06-01' then 1 else 2 end as member_cate		--���ϻ�Ա��ʶ
	from ds_crm.tp_cu_customerbase t1
	left join t1_1 t2
	where t1.create_time<'2018-06-01'
)
,
t1 as
(
	
)

-----------------------------------Ȼ�󣬵õ�20190601��Ա��ʼ������---------------------------
--


---------------------------

,
t5 as (
	select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT		--ÿ�����ѽ��
		,row_number() OVER (PARTITION BY member_id ORDER BY stsc_date asc) rk
	from (select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT from t1 s where SALE_AMOUNT>0
		)s
)
,

t6 as 
(
	select 
	member_id,
	min(stsc_date)stsc_date,
	case when SALE_AMOUNT<60 then 1
	when SALE_AMOUNT>=60 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<500 then 3
	when SALE_AMOUNT>=500 and SALE_AMOUNT<1000 then 4
	when SALE_AMOUNT>=1000 and SALE_AMOUNT<1800 then 5
	when SALE_AMOUNT>=1800 and SALE_AMOUNT<3000 then 6
	when SALE_AMOUNT>=3000 and SALE_AMOUNT<5000 then 7
	when SALE_AMOUNT>=5000 then 8 end as LV 
	from
	(
		select 
		t.member_id
		,t.stsc_date
		,sum(t1.SALE_AMOUNT) SALE_AMOUNT
		from t5 t
		left join t5 t1 on t1.member_id=t.member_id and t.rk>=t1.rk
		group by t.member_id
		,t.stsc_date
	)
	group by member_id,
	case when SALE_AMOUNT<60 then 1
	when SALE_AMOUNT>=60 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<500 then 3
	when SALE_AMOUNT>=500 and SALE_AMOUNT<1000 then 4
	when SALE_AMOUNT>=1000 and SALE_AMOUNT<1800 then 5
	when SALE_AMOUNT>=1800 and SALE_AMOUNT<3000 then 6
	when SALE_AMOUNT>=3000 and SALE_AMOUNT<5000 then 7
	when SALE_AMOUNT>=5000 then 8 end 
)	
,
--�����õ���ֵ
t8 as
(
	select T1.member_id
		,t1.LV
		,t2.LV as lv_2
		,case when t2.LV IS NULL THEN 0 ELSE DAYS_BETWEEN(t2.stsc_date,t1.stsc_date) end as day_diff
	from 
	(
		select member_id,
			stsc_date,
			LV,
			LV-1 AS LV_BEFORE
		from t6
		WHERE lv>1
	) t1 
	left join t6 t2
	on t1.member_id=t2.member_id
	and t1.LV_BEFORE=t2.LV
)
,t9 as
(
	select LV,sum(day_diff)/count(case when LV_2 is not null then member_id end)  as day_diff_avg
	from t8
	GROUP BY LV
)
select * from t9

 
--SUM(DAY_DIFF)/COUNT(1)
--L2 44.559233










