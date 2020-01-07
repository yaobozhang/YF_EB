--贡献者：姚泊彰
--时间：20200102
--目的：用于定义每段等级区间

--1、首先，确定累计销售占20%的分级点，得到最顶级会员
--得到每个会员成长值
with t1 as (
	select member_id
		,SALE_AMT
		,row_number() OVER(ORDER BY SALE_AMT DESC) AS rk
		,count(1) over() as total_memb
	from
	(
		SELECT member_id
			,SUM(SALE_AMT) AS SALE_AMT
		from "EXT_TMP"."YBZ_MEMBER_ANY"
		group by member_id 
		having sum(sale_amt) >1
	)
)
,
--得到每个会员按照销售排降序的累计销售值
t2 as (
	SELECT MEMBER_ID
		,SALE_AMT
		,rk/total_memb as memb_rk
		,SUM(SALE_AMT)OVER(ORDER BY SALE_AMT desc) AS SALE_PLUS  	--每一次累计金额
		,SUM(SALE_AMT)OVER() as SALE_TOTAL			--累计销售额
	FROM t1
)
--select * from t2 limit 10
--SELECT *,SALE_PLUS/SALE_TOTAL FROM t2 where SALE_PLUS/SALE_TOTAL>=0.8 order by SALE_AMT desc limit 10
SELECT *,SALE_PLUS/SALE_TOTAL FROM t2 where memb_rk>=0.2 order by SALE_AMT desc limit 10
--前20%会员成长值点：759   销售占比75.9%
--80%销售贡献成长值拐点610  会员占比23.8%

--2、然后，确定每个会员前三次消费的分布情况
with t1 as (
	SELECT member_id
		,STSC_DATE
		,SUM(SALE_AMT) AS SALE_AMT
	from "EXT_TMP"."YBZ_MEMBER_ANY"
	group by member_id
			,STSC_DATE
	having sum(sale_amt) >0
)
,
t2 as (
	SELECT SALE_AMT
		,SALE_RANK
	FROM
	(
		SELECT member_id
			,STSC_DATE
			,SALE_AMT
			,ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY STSC_DATE ASC) AS SALE_RANK	--订单排序，取前三单
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
select * from t4

--三次平均消费
/*
SALE_RANK;AVG(SALE_AMT)
1;96.3019
2;86.8392
3;85.1923
*/


--3、最后，找到中间部分等级的拐点
--首先，得到100块间隔会员的年龄分布
WITH t1 as (
	SELECT member_id
		,ceiling(SUM(SALE_AMT)/100)*100 AS SALE_AMT	--100向上取整
	from "EXT_TMP"."YBZ_MEMBER_ANY"
	group by member_id 
	having sum(sale_amt) >0

)
,
--拿到年龄分段,取年龄段在20到90之间
t2 as (
	select member_id
		,SALE_AMT
		,FLOOR(AGE/10)*10 AS AGE
	FROM
	(
		select t1.member_id
			,t1.SALE_AMT
			,'2018'-to_char(t2.birt_date,'YYYY') as age
		from t1
		inner join dw.fact_member_base t2
		on t1.member_id=t2.memb_code
	)
	where AGE>=20 and AGE<=90
)
,
--得到每段年龄成长值分布曲线
t3 as (
	SELECT SALE_AMT
		,AGE
		,COUNT(1) AS SALE_AGE_MEMB_NUM
		,SUM(SALE_AMT)OVER(PARTITION BY SALE_AMT) AS SALE_AMT_MEMB_NUM
	FROM t2
	GROUP BY SALE_AMT
		,AGE
)
SELECT SALE_AMT
		,AGE
		,SALE_AGE_MEMB_NUM/SALE_AMT_MEMB_NUM
FROM t3


--然后，得到高等级会员开始购买中药饮片,PROD_CATE_LEV1_CODE='Y02' and PROD_CATE_LEV4_NAME!='散装类'
--或者器械，PROD_CATE_LEV1_CODE='Y05' 
--AND PROD_CATE_LEV2_CODE!='Y0504'--功能性贴膏，如创可贴
--AND PROD_CATE_LEV2_CODE!='Y0506'--性保健用品，如避孕套
--AND PROD_CATE_LEV3_CODE!='Y050801'--普通型一次性耗材，如棉签等
--AND PROD_CATE_LEV4_CODE!='Y05020103'--血糖试纸
--或者大保健品的拐点，(PROD_CATE_LEV1_CODE='Y03' AND PROD_CATE_LEV3_NAME!='清咽润喉') OR (PROD_CATE_LEV1_CODE='Y09' AND PROD_CATE_LEV2_NAME='一般保健品')
--首先，得到高等级会员
WITH t1 as (
	SELECT member_id
	from "EXT_TMP"."YBZ_MEMBER_ANY1"
	group by member_id 
	having sum(sale_amt) >=600		--高等级划分点
	
)
,
--对商品进行打标，适应各种不同品类等级变化
t2 as (
	select member_id
		,STSC_DATE
		,SALE_AMT
		,GOODS_NAME
		,PROD_CATE_LEV2_CODE
		,PROD_CATE_LEV3_CODE
		,PROD_CATE_LEV4_CODE
		,PROD_CATE_LEV4_NAME
		,CASE WHEN PROD_CATE_LEV1_CODE='Y02' and PROD_CATE_LEV4_NAME!='散装类' THEN 1 ELSE 0 END AS CHINA_MEDI_FLAG	--中药
		,CASE WHEN PROD_CATE_LEV1_CODE='Y05' AND PROD_CATE_LEV2_CODE NOT IN ('Y0504','Y0506') AND PROD_CATE_LEV3_CODE!='Y050801' AND PROD_CATE_LEV4_CODE!='Y05020103' THEN 1 ELSE 0 END AS INSTRUMENT_FLAG --器械
		,CASE WHEN (PROD_CATE_LEV1_CODE='Y03' AND PROD_CATE_LEV3_NAME!='清咽润喉') OR (PROD_CATE_LEV1_CODE='Y09' AND PROD_CATE_LEV2_NAME='一般保健品') THEN 1 ELSE 0 END AS GNC_FLAG--保健品
		,CASE WHEN (PROD_CATE_LEV2_CODE IN ('Y0202'		--参茸贵细类
											)											
		OR PROD_CATE_LEV3_CODE IN ('Y050301'	--轮椅类
									,'Y050702') 	--健身类
		OR PROD_CATE_LEV4_CODE IN ('Y03011201'		--角鲨烯
									,'Y03011409'				--铁皮枫斗类
									,'Y03010601'				--阿胶类
									,'Y03011003'				--胶原蛋白类
									,'Y03020501'				--蛋白质类
									,'Y05010101'	--电子血压计
									,'Y05020102'	--血糖仪
									,'Y05050105'	--颈椎治疗仪
									,'Y05050204'	--腰椎治疗仪
									,'Y05050301'	--制氧机
									,'Y05050302'	--制氧器
									,'Y05050303'	--雾化器
									,'Y05050401'	--低中频治疗仪
									,'Y05050406'	--助听器
									,'Y05070101'	--测脂仪
									,'Y05070102'	--耳温枪
									,'Y05090301'	--病床
									,'Y05100103'	--中药煎药机
									,'Y05120101') 	--保健按摩类
		OR	PROD_CATE_LEV4_NAME ='超微饮片'
		)
		AND PROD_CATE_LEV4_NAME !='散装类' and t2.price>=150	--最高零售价大于150
		THEN 1 ELSE 0 END AS HIGH_VALUE_FLAG	--高单价中药、器械、保健品
		,case when goods_name like '%体温计%' then 1 else 0 end as TEMPETATURE_FLAG
	from "EXT_TMP"."YBZ_MEMBER_ANY1" t1
	left join (
		select goods_code
		,max(SUGG_RETAIL_PRIC) as price
		from
		"DW"."DIM_GOODS_COMPANY"
		group by goods_code
	)t2
	on t1.goods_code=t2.goods_code
)
,
t3 as (
	SELECT member_id
		,STSC_DATE
		,SUM(SALE_AMT) AS SALE_AMT
		,max(CHINA_MEDI_FLAG) as CHINA_MEDI_FLAG
		,max(INSTRUMENT_FLAG) as INSTRUMENT_FLAG
		,max(GNC_FLAG) as GNC_FLAG
		,MAX(HIGH_VALUE_FLAG) AS HIGH_VALUE_FLAG
		,max(TEMPETATURE_FLAG) as TEMPETATURE_FLAG
	from t2 t1
	where exists(
		select 1
		from t1 t2
		where t1.member_id=t2.member_id
	)
	group by member_id
			,STSC_DATE
	having sum(sale_amt) >0
)
,
t4 as (
	select member_id
		,STSC_DATE
		,sale_amt
		,SUM(SALE_AMT)OVER(partition by member_id ORDER BY STSC_DATE asc) AS SALE_total  	--每一次累计金额
		,CHINA_MEDI_FLAG
		,INSTRUMENT_FLAG
		,GNC_FLAG
		,HIGH_VALUE_FLAG
		,TEMPETATURE_FLAG
	from t3
)
--select * from t4 order by member_id,STSC_DATE limit 10
--统计每段成长值的消费会员数
,t5 as (
	SELECT MEMBER_ID
		,STSC_DATE
		,SALE_AMT
		,CHINA_MEDI_FLAG		--中药
		,INSTRUMENT_FLAG		--器械
		,GNC_FLAG				--保健品
		,HIGH_VALUE_FLAG		--高单价中药、保健品、器械
		,TEMPETATURE_FLAG
		,ceiling(SALE_TOTAL/10)*10 AS SALE_TOTAL	--100向上取整
	FROM t4
	WHERE SALE_TOTAL<10000
)
,
--统计中药会员数
t6_1 as (
	select SALE_TOTAL
		,count(distinct member_id) as memb_num
	from t5
	where CHINA_MEDI_FLAG=1 
	group by SALE_TOTAL
)
,
--统计器械会员数
t6_2 as (
	select SALE_TOTAL
		,count(distinct member_id) as memb_num
	from t5
	where INSTRUMENT_FLAG=1 
	group by SALE_TOTAL
)
,
--统计保健品会员数
t6_3 as (
	select SALE_TOTAL
		,count(distinct member_id) as memb_num
	from t5
	where GNC_FLAG=1 
	group by SALE_TOTAL
)
,
--统计买高单价商品会员数
t6_4 as (
	select SALE_TOTAL
		,count(distinct member_id) as memb_num
	from t5
	where HIGH_VALUE_FLAG=1 
	group by SALE_TOTAL
)
,
--统计买高单价商品会员数，每个会员只算第一单
t6_5 as (
	select SALE_TOTAL
	,count(distinct member_id) as memb_num
	FROM
	(
		select member_id
			,stsc_date
			,SALE_TOTAL
			,ROW_NUMBER() OVER(PARTITION BY member_id ORDER BY STSC_DATE ASC) AS SALE_RANK		--只取第一单
		from t5
		where HIGH_VALUE_FLAG=1 
	)
	WHERE SALE_RANK=1	
	group by SALE_TOTAL
)
,
--300买的什么中药
t7_1 as (
	select goods_name
		,count(distinct member_id) as memb_num
	from t2 t1
	where exists
	(
		select 1
		from t5 t2
		where t2.CHINA_MEDI_FLAG=1 
			and t2.SALE_TOTAL=300
			and t1.member_id=t2.member_id
			and t1.stsc_date=t2.stsc_date
	)
	and CHINA_MEDI_FLAG=1
	group by goods_name

)
--400买的什么器械？
,
t7_2 as (
	select goods_name
		,count(distinct member_id) as memb_num
	from t2 t1
	where exists
	(
		select 1
		from t5 t2
		where t2.INSTRUMENT_FLAG=1 
			and t2.SALE_TOTAL=400
			and t1.member_id=t2.member_id
			and t1.stsc_date=t2.stsc_date
	)
	and INSTRUMENT_FLAG=1 
	group by goods_name

)

--200买的什么保健品？

,
t7_3 as (
	select goods_name
		,count(distinct member_id) as memb_num
	from t2 t1
	where exists
	(
		select 1
		from t5 t2
		where t2.GNC_FLAG=1 
			and t2.SALE_TOTAL=200
			and t1.member_id=t2.member_id
			and t1.stsc_date=t2.stsc_date
	)
	and GNC_FLAG=1
	group by goods_name

)
,
--1000买的什么大额商品？

t7_4 as (
	select goods_name
		,max(PROD_CATE_LEV2_CODE) as PROD_CATE_LEV2_CODE
		,max(PROD_CATE_LEV3_CODE) as PROD_CATE_LEV3_CODE
		,max(PROD_CATE_LEV4_CODE) as PROD_CATE_LEV2_CODE
		,max(PROD_CATE_LEV4_name) as PROD_CATE_LEV4_name
		,count(distinct member_id) as memb_num
	from t2 t1
	where exists
	(
		select 1
		from t5 t2
		where t2.HIGH_VALUE_FLAG=1 
			and t2.SALE_TOTAL=1000
			and t1.member_id=t2.member_id
			and t1.stsc_date=t2.stsc_date
	)
	and HIGH_VALUE_FLAG=1
	group by goods_name

)
,
--器械400处拐点下探，买了体温计的这些人都是些什么人？他们为什么会买体温计，家里有没有小孩？
t8_1 as (
	select member_id
		,goods_name
	from t2 t1
	where exists
	(
		select 1
		from t5 t2
		where t2.INSTRUMENT_FLAG=1 
			and t2.SALE_TOTAL=400
			and t2.TEMPETATURE_FLAG =1
			and t1.member_id=t2.member_id
	)
	group by member_id
		,goods_name

)
,
--看看这些人都是什么年龄段的人
--有没有小孩
t8_2 as (
	select	member_id
		,child_flag
		,'2018'-to_char(t2.birt_date,'YYYY') as age
	from
	(
		select member_id
			,max(child_flag) as child_flag	--有没有小孩 
		from
		(
			select member_id
				,case when goods_name like '%小儿%' then 1 else 0 end as child_flag
			from t8_1
		)
		group by member_id
	)t1
	inner join dw.fact_member_base t2
	on t1.member_id=t2.memb_code
)
,
--对年龄进行处理，然后统计
t8_3 as (
	select age
		,child_flag
		,count(1) as memb_num
	from
	(
		select member_id
			,child_flag
			,case when age>=20 and age <=90 then FLOOR(AGE/10)*10 else null end AS AGE
		from t8_2
	)
	group by age 
		,child_flag
)
--select * from t8_3

--SELECT * FROM t6_4 ORDER BY SALE_TOTAL ASC
select * from t7_4 ORDER BY memb_num DESC












