--会员平均生命周期及再复购数据分析
--代码贡献者：姚泊彰
--代码更新时间：20191226
--数据口径：见各自模块


--STEP1:得到会员平均生命周期数据
DROP TABLE "EXT_TMP"."YBZ_MEMBER";
create column table "EXT_TMP"."YBZ_MEMBER" as
(

		SELECT MEMBER_ID
			,CASE WHEN SALE_DAY_NUM>1 THEN DAYS_BETWEEN(first_sale_date,last_sale_date)
				ELSE 1 END AS MEMB_LIFE_DAY_NUM_1
			,CASE WHEN SALE_DAY_NUM>1 THEN DAYS_BETWEEN(first_sale_date,last_sale_date)
				ELSE 0 END AS MEMB_LIFE_DAY_NUM_2
			,SALE_AMT
			,SALE_DAY_NUM
			FROM
		(
			SELECT MEMBER_ID
				,max(stsc_date) as last_sale_date
				,min(stsc_date) as first_sale_date
				,count(1) as sale_day_num
				,SUM(SALE_AMT) AS SALE_AMT					--消费金额
			FROM (
				SELECT
					 "STSC_DATE",  								--销售日期
					 "MEMBER_ID",								--会员编码
					SUM("SALE_AMT") AS SALE_AMT					--消费金额
				FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					 '20150101'),
					 'PLACEHOLDER' = ('$$BeginTime$$',
					 '20140101')) 
				where "MEMBER_ID" is not null
				GROUP BY "STSC_DATE",                                  --销售日期
					 "MEMBER_ID" 
			)
			group by MEMBER_ID
		)
	
);
commit;

/*--1、得到会员平均生命周期*/
select SUM(to_number(MEMB_LIFE_DAY_NUM_1))/COUNT(MEMBER_ID) LIFE_DAY_1	--消费1次的算一天
	,SUM(to_number(MEMB_LIFE_DAY_NUM_2))/COUNT(CASE WHEN MEMB_LIFE_DAY_NUM_2 >0 THEN MEMBER_ID END) LIFE_DAY_2 --消费1次的不算
	,AVG(SALE_AMT) AS SALE_AMT_1			--平均消费金额
	,SUM(CASE WHEN SALE_DAY_NUM>1 THEN SALE_AMT END)/COUNT(CASE WHEN SALE_DAY_NUM>1 THEN MEMBER_ID END) AS SALE_AMT_2	--平均消费金额（消费1次不算）
FROM "EXT_TMP"."YBZ_MEMBER";




--STEP2:得到会员复购再复购数据
--得到会员每年数据并建表
/*
CREATE PROCEDURE ext_tmp.YBZ_MEMB_REBUY  ()
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
--call ext_tmp.YBZ_MEMB_REBUY()
BEGIN
--得到会员每年数据并建表 
VAR1=
	SELECT AT_YEAR
		,MEMBER_ID
		,SALE_AMT
		--,row_number() OVER (PARTITION BY member_id ORDER BY AT_YEAR asc) rk
	FROM
	(
		SELECT
			 to_char("STSC_DATE",'YYYY') as at_year,  								--销售日期
			 t1."MEMBER_ID",								--会员编码
			SUM("SALE_AMT") AS SALE_AMT					--消费金额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191231'),							--两年两年跑
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t1
		GROUP BY to_char("STSC_DATE",'YYYY'),                                  --销售日期
			 t1."MEMBER_ID" 
	)t1
	where exists(
		select 1 from dw.fact_member_base t2		--找到已有会员
		where t1.MEMBER_ID=t2.MEMB_CODE
	)

;

create column table "EXT_TMP"."YBZ_MEMBER_1" as
(
	SELECT AT_YEAR
		,MEMBER_ID
		,SALE_AMT
	FROM :VAR1
)
;

insert into "EXT_TMP"."YBZ_MEMBER_1"
(
	AT_YEAR
	,MEMBER_ID
	,SALE_AMT
)
SELECT AT_YEAR
	,MEMBER_ID
	,SALE_AMT
FROM :VAR1
COMMIT;


END;
*/
--从表中开始计算年复购情况
with t1 as (
	SELECT AT_YEAR
		,MEMBER_ID
		,SALE_AMT
		,row_number() OVER (PARTITION BY member_id ORDER BY AT_YEAR asc) rk
	from "EXT_TMP"."YBZ_MEMBER_1"
)
,
t2 as (
	select t1.MEMBER_ID			--会员号
		,to_number(YEARS_BETWEEN(t2.AT_YEAR,t1.AT_YEAR)) as year_rk	--年份编码
		,t1.AT_YEAR
		,t1.SALE_AMT			--消费金额
	FROM t1
	left join
	(
		SELECT MEMBER_ID,AT_YEAR
		FROM t1
		WHERE rk=1
	)t2
	on t1.MEMBER_ID=t2.MEMBER_ID

)
,
--找到第1年购买的人
t2_1 as (
	select member_id,year_rk,at_year,sale_amt
	from t2
	where year_rk=0
)
--得到每个会员每年是否连续购买
t3 as (
	select t1.MEMBER_ID
		,t1.year_rk
		,t1.AT_YEAR
		,t1.SALE_AMT
		,case when t2.member_id is not null then 1 else 0 end as two_year_buy_flag
		,t2.SALE_AMT as two_year_sale_amt
	FROM T2_1 T1
	LEFT JOIN (
		--找到第2年购买的人
		select member_id,year_rk,at_year,sale_amt
		from t2
		where year_rk=1
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
,
--得到每个会员跨年是否购买
t4 as (
	select t1.MEMBER_ID
		,t1.year_rk
		,t1.AT_YEAR
		,t1.SALE_AMT
		,t1.two_year_buy_flag
		,t1.two_year_sale_amt
		,case when t2.member_id is not null then 1 else 0 end as three_year_buy_flag
		,t2.SALE_AMT as three_year_sale_amt
	FROM T3 T1
	LEFT JOIN (
		--找到第3年购买的人
		select member_id,year_rk,at_year,sale_amt
		from t2
		where year_rk=2
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
,
--得到每个会员第三年是否购买
t5 as (
	select t1.MEMBER_ID
		,t1.year_rk
		,t1.AT_YEAR
		,t1.SALE_AMT
		,t1.two_year_buy_flag
		,t1.two_year_sale_amt
		,t1.three_year_buy_flag
		,t1.three_year_sale_amt
		,case when t2.member_id is not null then 1 else 0 end as four_year_buy_flag
		,t2.SALE_AMT as four_year_sale_amt
	FROM T4 T1
	LEFT JOIN (
		--找到第4年购买的人
		select member_id,year_rk,at_year,sale_amt
		from t2
		where year_rk=3
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
,
--得到每个会员第四年是否购买
t6 as (
	select t1.MEMBER_ID
		,t1.year_rk
		,t1.AT_YEAR
		,t1.SALE_AMT
		,t1.two_year_buy_flag
		,t1.two_year_sale_amt
		,t1.three_year_buy_flag
		,t1.three_year_sale_amt
		,t1.four_year_buy_flag
		,t1.four_year_sale_amt
		,case when t2.member_id is not null then 1 else 0 end as five_year_buy_flag
		,t2.SALE_AMT as five_year_sale_amt
	FROM T5 T1
	LEFT JOIN (
		--找到第5年购买的人
		select member_id,year_rk,at_year,sale_amt
		from t2
		where year_rk=4
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
,
--得到每个会员第五年是否购买
t7 as (
	select t1.MEMBER_ID
		,t1.year_rk
		,t1.AT_YEAR
		,t1.SALE_AMT
		,t1.two_year_buy_flag
		,t1.two_year_sale_amt
		,t1.three_year_buy_flag
		,t1.three_year_sale_amt
		,t1.four_year_buy_flag
		,t1.four_year_sale_amt
		,t1.five_year_buy_flag
		,t1.five_year_sale_amt
		,case when t2.member_id is not null then 1 else 0 end as six_year_buy_flag
		,t2.SALE_AMT as six_year_sale_amt
	FROM T6 T1
	LEFT JOIN (
		--找到第6年购买的人
		select member_id,year_rk,at_year,sale_amt
		from t2
		where year_rk=5
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID
)
,

--复购再复购数据
t8 as(
	select SUM(SALE_AMT)/COUNT(MEMBER_ID)			--第1年购买的金额
		,count(case when AT_YEAR<'2019' AND two_year_buy_flag=1 then MEMBER_ID END)
		/count(case when AT_YEAR<'2019' then MEMBER_ID END) AS SECOND_CNSM_RATE		--第1年购买了第二年还会购买的概率
		
		,SUM(case when AT_YEAR<'2019' AND two_year_buy_flag=1 then two_year_sale_amt END)
		/count(case when AT_YEAR<'2019' AND two_year_buy_flag=1 then MEMBER_ID END) AS SECOND_CNSM_AMT		--第1年购买了第二年还会购买的金额
		
		,count(case when AT_YEAR<'2018' AND two_year_buy_flag=1 AND three_year_buy_flag=1 then MEMBER_ID END)
		/count(case when AT_YEAR<'2018' AND two_year_buy_flag=1 then MEMBER_ID END) AS THIRD_CNSM_RATE		--第1年购买了第二年还会购买第三年还会购买的概率
		
		,SUM(case when AT_YEAR<'2018' AND two_year_buy_flag=1 AND three_year_buy_flag=1  then three_year_sale_amt END)
		/count(case when AT_YEAR<'2018' AND two_year_buy_flag=1 AND three_year_buy_flag=1  then MEMBER_ID END) AS THIRD_CNSM_AMT		--第1年购买了第二年还会购买第三年还会购买的金额
		
		,count(case when AT_YEAR<'2017' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1 then MEMBER_ID END)
		/count(case when AT_YEAR<'2017' AND two_year_buy_flag=1 AND three_year_buy_flag=1 then MEMBER_ID END) AS FORTH_CNSM_RATE		--第1年购买了第二年还会购买第三年还会购买第四年还会购买的概率
		
		,SUM(case when AT_YEAR<'2017' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1  then four_year_sale_amt END)
		/count(case when AT_YEAR<'2017' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1 then MEMBER_ID END) AS FORTH_CNSM_AMT		--第1年购买了第二年还会购买第三年还会购买第四年还会购买的金额
		
		,count(case when AT_YEAR<'2016' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1  AND five_year_buy_flag=1  then MEMBER_ID END)
		/count(case when AT_YEAR<'2016' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1  then MEMBER_ID END) AS FIFTH_CNSM_RATE		--第1年购买了第二年还会购买第三年还会购买第四年还会购买第五年还会购买的概率
		
		,SUM(case when AT_YEAR<'2016' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1  AND five_year_buy_flag=1 then three_year_sale_amt END)
		/count(case when AT_YEAR<'2016' AND two_year_buy_flag=1 AND three_year_buy_flag=1 AND four_year_buy_flag=1  AND five_year_buy_flag=1  then MEMBER_ID END) AS FIFTH_CNSM_AMT		--第1年购买了第二年还会购买第三年还会购买第四年还会购买第五年还会购买的金额
					
	from t7
	where SALE_AMT>0
)
SELECT * FROM t8






	