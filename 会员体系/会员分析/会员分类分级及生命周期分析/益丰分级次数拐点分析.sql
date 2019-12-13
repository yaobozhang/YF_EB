with t1_1 as
(
	select member_id
		,stsc_date
		,phmc_code
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
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
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
	,stsc_date
	,phmc_code
	having SUM(SALE_AMT)>0
)
,
t1_2 as
(
	select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT		--ÿ�����ѽ��
		,case when t2.FIR_DISEASE_CODE='D002' then 1 else 0 end AS D002		--�ʺ���
		,case when t2.FIR_DISEASE_CODE='D005' then 1 else 0 end AS D005		--����
		,case when t2.FIR_DISEASE_CODE in ('D004','D006','D007','D008') then 1 else 0 end AS D004678	--����
	from t1_1 s
	left join 
	"DM"."MEMBER_FOCU_DISEASE_LAB" t2 on 
	s.member_id=t2.member_id
	where not exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and c.come_from in ('SG','JM')
	)
	AND exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and create_time<'2018-06-01'
	)
)
,
--ͳ�Ƶõ����ѽ����Ѵ���
t1 as
(
	select member_id,
		sum(sale_amount) as sale_amount,		--���ѽ��
		count(1) as sale_times,				--���Ѵ���
		max(D002) as D002,--�ʺ���
		max(D005) as D005,--����
		max(D004678) as D004678--����
	from t1_2
	group by member_id
)
,
--����ÿ���ɳ�ֵ��Ա�ĳɳ�ֵ�����ۼ����Ѵ���
t2_1 as
(
	select member_id,
		floor(SALE_AMOUNT) as gro_point,	--20190601�ɳ�ֵ
		sale_times,
		SALE_AMOUNT,
		D002,--�ʺ���
		D005,--����
		D004678--����
	from
	(
	select t1.member_id
		,t1.SALE_AMOUNT
		,t1.sale_times	
		,D002,--�ʺ���
		D005,--����
		D004678--����	
	from t1
	)
)
,
--����ɳ�ֵ�Σ���100Ϊ���
t2_2 as
(
	select (floor(gro_point/100)+1)*100 as gro_point_stage
	,member_id
	,sale_amount
	,sale_times
	,D002,--�ʺ���
	D005,--����
	D004678--����
	,count(1) over() as memb_total
	,sum(sale_amount) over()	as sale_total
	from t2_1
)
,
--����ÿ���ɳ�ֵ�ε�ƽ�����ѽ�ƽ��ÿ����Աÿ�����ѽ��
t2 as
(
	select gro_point_stage
		,sum(sale_amount)/count(member_id)/avg(sale_times) as memb_avgtime_amount
		,avg(sale_times) as avg_sale_times
		,count(member_id)/max(memb_total) as memb_rate
		,sum(sale_amount)/max(sale_total) as sale_rate
		,sum(D002)/count(member_id) as D002_RATE
		,sum(D005)/count(member_id) as D005_RATE
		,sum(D004678)/count(member_id) as D002_RATE
	from t2_2
	group by gro_point_stage
)
select * from t2 order by gro_point_stage asc

--step2:���㲻ͬ�ȼ�����Ϊ�յ㣬����Ʒ��ռ�ȣ�SKU����Ӫ��Ȩ��ռ��
--����SKU��Ӫ��Ȩ��ռ��
with t1_1 as
(
	select member_id
		,count(distinct s.GOODS_CODE) as SKU_NUM		--SKU��
		,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--Ӫ������
		,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--��Ӫ��
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
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
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
	having SUM(SALE_AMT)>0
)
,
t1_2 as
(
	select s.member_id
		,SKU_NUM
		,PURC_MONEY
		,NO_PURC_MONEY
		,SALE_AMOUNT		--ÿ�����ѽ��
		,case when SALE_AMOUNT<100 then 1
		when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
		when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
		when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
		when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
		when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
		when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
		when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
		when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
		when SALE_AMOUNT>=4600 THEN 10 end as LV 
	from t1_1 s
	where not exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and c.come_from in ('SG','JM')
	)
	AND exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and create_time<'2018-06-01'
	)
)
SELECT LV
	,SUM(SKU_NUM)/COUNT(member_id) AS AVG_SKU_NUM		--ƽ��ÿ����Ա����SKU��
	,SUM(PURC_MONEY)/(SUM(PURC_MONEY)+SUM(NO_PURC_MONEY)) AS PURC_RATE		--Ӫ��Ȩ��ռ��
FROM t1_2 
GROUP BY LV

--����Ʒ��ռ��
--���ȵõ�1����2��Ʒ��
with t1_1 as
(
	select s.member_id
		,s.GOODS_CODE
		,g.PROD_CATE_LEV1_CODE
		,g.PROD_CATE_LEV1_NAME
		,g.PROD_CATE_LEV2_CODE
		,g.PROD_CATE_LEV2_NAME
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿƷ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
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
	group by member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
		,s.GOODS_CODE
		,g.PROD_CATE_LEV1_CODE
		,g.PROD_CATE_LEV1_NAME
		,g.PROD_CATE_LEV2_CODE
		,g.PROD_CATE_LEV2_NAME
	having SUM(SALE_AMT)>0
)
,
--�õ�ƴ�Ӻ��Ʒ��
t1_2 as
(
	select member_id
		,PROD_CATE_LEV2_NAME
		,sum(SALE_AMOUNT) as SALE_AMOUNT
	from
	(
		select member_id
		,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'����ҩ'
			  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'�Ǵ���ҩ'
			  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'����') END PROD_CATE_LEV2_NAME
		,SALE_AMOUNT
		from t1_1 s
		left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
		on S.GOODS_CODE = T.GOODS_CODE
	)
	group by member_id
		,PROD_CATE_LEV2_NAME
)
,
--���˲��õ�ÿ����Ա�ȼ�
t1_3 as
(
	SELECT MEMBER_ID
		,case when SALE_AMOUNT<100 then 1
		when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
		when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
		when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
		when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
		when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
		when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
		when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
		when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
		when SALE_AMOUNT>=4600 THEN 10 end as LV
	FROM(
		select s.member_id
		,SUM(SALE_AMOUNT) AS SALE_AMOUNT	--ÿ�����ѽ��
		from t1_1 s
		where not exists
		(
			select 1 from ds_crm.tp_cu_customerbase c
			where s.member_id=c.customer_id
			and c.come_from in ('SG','JM')
		)
		AND exists
		(
			select 1 from ds_crm.tp_cu_customerbase c
			where s.member_id=c.customer_id
			and create_time<'2018-06-01'
		)group by member_id
	)
	
)
,
--�����õ�ÿ��Ʒ��ռ��ֵ
t1_4 as
(
	select T1.member_id
		,T1.LV 
		,T2.PROD_CATE_LEV2_NAME
		,T2.SALE_AMOUNT
		,SUM(SALE_AMOUNT) OVER(PARTITION BY LV,PROD_CATE_LEV2_NAME) AS LV_CATE_SALE
		,SUM(SALE_AMOUNT) OVER(PARTITION BY LV) AS LV_SALE
	from t1_3 T1
	LEFT JOIN t1_2 T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
SELECT LV
	,PROD_CATE_LEV2_NAME
	,max(LV_CATE_SALE)/max(LV_SALE) AS lv_cate_rate
FROM t1_4 
GROUP BY LV
	,PROD_CATE_LEV2_NAME


--�õ���Աÿ���ĸ�Ʒ������
with t1_1 as
(
	select s.member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
		,s.stsc_date
		,s.phmc_code
		,s.GOODS_CODE
		,g.PROD_CATE_LEV1_CODE
		,g.PROD_CATE_LEV1_NAME
		,g.PROD_CATE_LEV2_CODE
		,g.PROD_CATE_LEV2_NAME
		,SUM(SALE_AMT) AS SALE_AMOUNT		--ÿ�����ѽ��
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--Ӫ���޳���������Ʒ
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--Ӫ��ȥ��Ʒ��
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--Ӫ��ȥ����Ʒ
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--������Ա��������
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
	group by s.member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
	,s.stsc_date
	,s.phmc_code
	,s.GOODS_CODE
	,g.PROD_CATE_LEV1_CODE
	,g.PROD_CATE_LEV1_NAME
	,g.PROD_CATE_LEV2_CODE
	,g.PROD_CATE_LEV2_NAME
	having SUM(SALE_AMT)>0
)
,
--��Ա����
t1_2 as(
	select s.member_id	--ÿ����Աÿ����ÿ���ŵ���һ��
		,s.stsc_date
		,s.phmc_code
		,s.GOODS_CODE
		,s.PROD_CATE_LEV1_CODE
		,s.PROD_CATE_LEV1_NAME
		,s.PROD_CATE_LEV2_CODE
		,s.PROD_CATE_LEV2_NAME
		,s.SALE_AMOUNT		--ÿ�����ѽ��
	from t1_1 s
	where not exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and c.come_from in ('SG','JM')
	)
	AND exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and create_time<'2018-06-01'
	)
)
,
--�õ�ƴ��Ʒ��
t1 as
(
	select member_id
		,stsc_date
		,phmc_code
		,PROD_CATE_LEV2_NAME
		,sum(SALE_AMOUNT) as SALE_AMOUNT
	from
	(
		select member_id
			,stsc_date
			,phmc_code
		,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'����ҩ'
			  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'�Ǵ���ҩ'
			  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'����') END PROD_CATE_LEV2_NAME
		,SALE_AMOUNT
		from t1_1 s
		left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
		on S.GOODS_CODE = T.GOODS_CODE
	)
	group by member_id
		,stsc_date
		,phmc_code
		,PROD_CATE_LEV2_NAME
)
,
--�õ�ÿ��ÿ��ÿ�ŵ������ۼ�����
t1_0 as
(
	select member_id
		,stsc_date
		,phmc_code
		,SALE_AMOUNT
		,row_number() OVER (PARTITION BY member_id ORDER BY stsc_date asc) rk	--����ʱ���������ĸ��ŵ���ǰ����Ҫ
	from
	(
		select member_id
			,stsc_date
			,phmc_code
			,sum(SALE_AMOUNT) as SALE_AMOUNT
		from t1
		group by 
			member_id
			,stsc_date
			,phmc_code
	)
)
,
--�õ���Աÿ����Ӧ�ļ���
t2 as
(
	select member_id
	,stsc_date
	,phmc_code
	,case when SALE_AMOUNT<100 then 1
	when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
	when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
	when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
	when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
	when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
	when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
	when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
	when SALE_AMOUNT>=4600 THEN 10 end as LV
	from
	(
		select member_id
			,stsc_date
			,phmc_code
			,sum(SALE_AMOUNT) as SALE_AMOUNT
		from
		(
			select t.member_id
			,t.stsc_date
			,t.phmc_code
			,t1.SALE_AMOUNT
			from t1_0 t
			left join t1_0 t1 
			on t.member_id=t1.member_id and t.rk>=t1.rk
		)
		group by member_id
		,stsc_date
		,phmc_code
	)
)
,
--�õ���Աÿ����������ռ����ߵ�Ʒ��
--�����õ�ÿ��ÿƷ���Ӧ�ȼ�
t3_1 as
(
	select t1.member_id
		,t1.stsc_date
		,t1.phmc_code
		,t1.PROD_CATE_LEV2_NAME
		,t1.SALE_AMOUNT
		,t2.lv
	from t1
	left join t2
	on t1.member_id=t2.member_id
	and t1.stsc_date=t2.stsc_date
	and t1.phmc_code=t2.phmc_code
)
,
t3 as
(
	SELECT member_id
		,lv
		,PROD_CATE_LEV2_NAME
		,row_number() OVER (PARTITION BY member_id ORDER BY lv aSc) rk
	FROM
	(
		SELECT member_id
			,lv
			,PROD_CATE_LEV2_NAME
			,row_number() OVER (PARTITION BY member_id,LV ORDER BY SALE_AMOUNT DESc) rk
		FROM
		(
			select member_id
				,lv
				,PROD_CATE_LEV2_NAME
				,sum(SALE_AMOUNT) AS SALE_AMOUNT
			from t3_1
			group by member_id
				,lv
				,PROD_CATE_LEV2_NAME
		)
	)
	WHERE RK=1
)
,
--�õ�������תֵ
t4 AS
(
	SELECT lv_before
		,cate_before
		,lv_after
		,cate_after
		,jump_num --��ת����
		,SUM(jump_num) OVER (PARTITION BY lv_before,cate_before,lv_after) TOTAL_NUM
		,jump_num/TOTAL_NUM as jump_rate	--��ת����
	FROM
	(
		--�õ�ÿ������ÿ��Ʒ��������
		SELECT lv_before
			,cate_before
			,lv_after
			,cate_after
			,jump_num --��ת����
			,SUM(jump_num) OVER (PARTITION BY lv_before,cate_before) TOTAL_NUM
		FROM
		(
			--�õ�ÿ��������ת�ĸ���
			select lv_before
				,cate_before
				,lv_after
				,cate_after
				,count(1) as jump_num--��ת����
			from
			(
				--�õ�ÿ����Ա��ǰ���𼰺�һ������
				select t1.member_id
					,t1.lv as lv_before
					,t1.PROD_CATE_LEV2_NAME as cate_before
					,t2.lv as lv_after
					,t2.PROD_CATE_LEV2_NAME as cate_after
				from t3 t1
				left join t3 t2
				on t1.member_id=t2.member_id
				and t1.rk+1=t2.rk
			)
			where lv_after is not null
			group by lv_before
				,cate_before
				,lv_after
				,cate_after
		)
	)
)

select * from t4 order by jump_num desc




















