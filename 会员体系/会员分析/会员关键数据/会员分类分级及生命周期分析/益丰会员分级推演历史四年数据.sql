--�ô���Ϊ��Ա�ּ������ƶ�����(��ʷ����)
--���빱���ߣ�Ҧ����
--ʱ�䣺201900704


-----------------------------------���ȣ��õ�20151231��Ա��ʼ������---------------------------
with t1 as
(
	select member_id
		,year(stsc_date) as data_year
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2016-01-01',-1)
	and s.stsc_date<'2016-01-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
		,year(stsc_date)
	having SUM(SALE_AMT)>0
)
,

-----------------------------------Ȼ�󣬵õ�20151231��Ա�ɳ�ֵ�ּ�����---------------------------
,

t2 as 
(
	select 
	member_id,
	data_year,
	SALE_AMOUNT,
	case when SALE_AMOUNT<100 then 1
	when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
	when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
	when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
	when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
	when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
	when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
	when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
	when SALE_AMOUNT>=4600 and SALE_AMOUNT<7000 then 10
	when SALE_AMOUNT>=7000 and SALE_AMOUNT<10000 then 11
	when SALE_AMOUNT>=10000 then 12 end as LV 
	from t1
)	
,
---------------------------------�õ����л�Ա��2016,2017,2018�ĳɳ�ֵ--------------------------------------
--STEP1:�õ����л�Ա��2016,2017,2018��������ֵ
t3 as
(
	select member_id
		,year(stsc_date) as data_year
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2017-01-01',-1)
	and s.stsc_date<'2019-01-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
		,year(stsc_date)
	having SUM(SALE_AMT)>0
)
,
--STEP2:ɸѡ����ʼ����Ա��������ֵ
t4 as 
(
	select member_id
		,data_year                                                     
		,SALE_AMOUNT
	from t3
	where exists(
		select 1 from t2 
		where t3.member_id=t2.member_id
	)
)
,
--STEP3:�Ի�Ա���д��
t5 as
(
	select 
	member_id,
	data_year,
	SALE_AMOUNT,
	case when SALE_AMOUNT<100 then 1
	when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
	when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
	when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
	when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
	when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
	when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
	when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
	when SALE_AMOUNT>=4600 and SALE_AMOUNT<7000 then 10
	when SALE_AMOUNT>=7000 and SALE_AMOUNT<10000 then 11
	when SALE_AMOUNT>=10000 then 12 end as LV 
	from t4
)


--STEP3:�õ�2016��2017��2018�����Ļ�Ա��2016,2017,2018�ĳɳ�ֵ








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










