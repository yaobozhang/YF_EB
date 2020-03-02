--该代码为会员分类分级数据分析部分
--代码贡献者：姚泊彰
--时间：20200109

--STEP1：得到会员等级情况

with t1 as 
(
	select 
	member_id,
	SALE_AMT,
	case when SALE_AMT <200 then 1
		when SALE_AMT >=200 and SALE_AMT <600 then 2
		when SALE_AMT >=600 and SALE_AMT <1000 then 3
		when SALE_AMT >=1000 and SALE_AMT <4300 then 4
		when SALE_AMT >=4300 and SALE_AMT <10000 then 5
		when SALE_AMT >=10000 then 6 end as LV 
	from
	(
		SELECT member_id
			,SUM(SALE_AMT) AS SALE_AMT
		from "DW"."FACT_SALE_ORDR_DETL"
		where member_id is not null
		and stsc_date>='20180101'
		and stsc_date<'20200101'
		group by member_id 
		having sum(sale_amt) >0
	)t1

)	
,
t2 as (
	select LV
		,count(member_id) as memb_num
		,sum(SALE_AMT) as SALE_AMT
	from t1
	group by LV
)
,
t3 as (
	select LV
		,MEMB_NUM
		,SALE_AMT
		,SUM(MEMB_NUM) OVER() as memb_num_total
		,SUM(SALE_AMT) OVER() as SALE_AMT_total
	from t2
)
SELECT LV,MEMB_NUM,SALE_AMT,MEMB_NUM/memb_num_total,SALE_AMT/SALE_AMT_total FROM t3 










