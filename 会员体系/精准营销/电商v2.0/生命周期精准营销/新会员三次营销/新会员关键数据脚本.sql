--贡献者：姚泊彰
--时间：20200108
--目的：用于给出新会员每单消费金额及毛利额情况


--1、拿到新增会员每单数据,同人同天同门店算一天
with t1 as (
	SELECT member_id
		,STSC_DATE
		,PHMC_CODE
		,MIN(ORDR_SALE_TIME) AS SALE_TIME
		,sum(GROS_PROF_AMT) AS GROS_PROF_AMT
		,SUM(SALE_AMT) AS SALE_AMT
	FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
		 '20191231'),
		 'PLACEHOLDER' = ('$$BeginTime$$',
		 '20180101')) t1
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
	SELECT SALE_AMT
		,GROS_PROF_AMT
		,SALE_RANK
	FROM
	(
		SELECT member_id
			,STSC_DATE
			,PHMC_CODE
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
		SALE_RANK,
		SALE_AMT,
		count(1)
	from 
	(
			SELECT SALE_RANK
				,FLOOR(SALE_AMT/10)*10  SALE_AMT
			FROM t2
			
	)
	group by SALE_RANK,SALE_AMT
	order by SALE_RANK,SALE_AMT
)
,
t4 as (
	select SALE_RANK
		,avg(SALE_AMT)
	from t2
	group by SALE_RANK
)
select * from t3











