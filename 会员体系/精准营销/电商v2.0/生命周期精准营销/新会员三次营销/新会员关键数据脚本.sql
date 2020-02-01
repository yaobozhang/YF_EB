--贡献者：姚泊彰
--时间：20200108
--目的：用于给出新会员每单消费金额及毛利额情况


--1、拿到新增会员每单数据,同人同天同门店算一天
with t1 as (
	SELECT t1.member_id
		,t1.STSC_DATE
		,t1.PHMC_CODE
		,max(t2.city) as city			--城市
		,MIN(ORDR_SALE_TIME) AS SALE_TIME
		,sum(GROS_PROF_AMT) AS GROS_PROF_AMT
		,SUM(SALE_AMT) AS SALE_AMT
	FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
		 '20200101'),
		 'PLACEHOLDER' = ('$$BeginTime$$',
		 '20180101')) t1
	inner join 
		(
			select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				city,			--常德市
				phmc_type
			from dw.dim_phmc
		) t2
	on t1.PHMC_CODE=t2.PHMC_CODE
	WHERE EXISTS(
		SELECT 1 FROM dw.fact_member_base t2
		where t1.member_id=t2.memb_code
		and t2.crea_time>='20180101'
	)
	group by member_id
			,STSC_DATE
			,PHMC_CODE
	having sum(sale_amt) >0
)
,
--对会员每单数据进行排序,取前三单
t2 as (
	SELECT member_id
		,city
		,SALE_AMT
		,GROS_PROF_AMT
		,SALE_RANK
	FROM
	(
		SELECT member_id
			,STSC_DATE
			,PHMC_CODE
			,city
			,ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY SALE_TIME ASC) AS SALE_RANK
			,GROS_PROF_AMT
			,SALE_AMT
		FROM t1
	)
	WHERE SALE_RANK<=3
)
,
t3 as (
	select 
		city,
		SALE_RANK,
		SALE_AMT,
		count(1) as memb_num
	from 
	(
			SELECT city
				,SALE_RANK
				,FLOOR(SALE_AMT/10)*10  SALE_AMT
			FROM t2
			
	)
	group by city,SALE_RANK,SALE_AMT
	order by city,SALE_RANK,SALE_AMT
)
,
t3_1 as (
	select t1.member_id
		,t1.SALE_AMT
		,t2.SALE_AMT as SALE_AMT_BEFORE
		,t1.SALE_RANK
	from t2 t1
	left join t2
	on t1.member_id=t2.member_id
	and t1.SALE_RANK=t2.SALE_RANK+1
)
,
t4 as (
	select SALE_RANK
		,avg(SALE_AMT)
	from t2
	group by SALE_RANK
)
,
t4_1 as (
	select member_id
		,case when SALE_AMT<=30 then 1
			when SALE_AMT>30 and SALE_AMT<=60 then 2
			when SALE_AMT>60 and SALE_AMT<=120 then 3
			when SALE_AMT>120 then 4 
			else 5 end as sale_flag
		,case when SALE_AMT_BEFORE<=30 then 1
			when SALE_AMT_BEFORE>30 and SALE_AMT_BEFORE<=60 then 2
			when SALE_AMT_BEFORE>60 and SALE_AMT_BEFORE<=120 then 3
			when SALE_AMT_BEFORE>120 then 4 
			else 5 end as sale_before_flag
		,SALE_RANK
	from t3_1 t1
)
select * from t3

select SALE_RANK,sale_before_flag,count(1) as memb_num
from t4_1
group by SALE_RANK,sale_before_flag










