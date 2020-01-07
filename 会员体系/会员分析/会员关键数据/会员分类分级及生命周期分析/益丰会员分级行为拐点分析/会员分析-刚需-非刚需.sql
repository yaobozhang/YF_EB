1.根据会员第一笔消费时间，取出所有历史第一笔消费在2018（2019）年以后的会员 
得到表A（会员编码，消费日期，商品编码，商品1级品类，商品2级品类，商品自定义品类，是否刚需，商品数量，商品金额）

2.根据表A计算所有会员累积消费金额以及本次消费所有品类以及本次消费商品件数以及是否刚需 
得到表B（会员编码，消费日期，第几次消费，消费金额，消费品类，消费件数，是否刚需）

3.根据表B 对会员打标，三种情况分别打标累积品类数、消费商品件数、刚需非刚需 
得到表C（会员编码，消费日期，消费次数，累积金额，消费品类，消费件数，是否刚需）

4.分别算出，每一个会员的三个分布基础数据

表X【会员单品类到多品类累积消费金额表】(会员编码，1品类日期，1品类累积消费金额，2品类日期，2品类消费金额，3品类日期，3品类消费金额)

表Y【会员单件到多件累积消费金额表】（会员编码，1件日期，1件累积消费金额，2件日期，2件累积消费金额，3件日期，3件累积消费金额）

表Z【会员刚需到非刚需累积消费金额表】（会员编码，刚需开始日期，刚需开始消费金额，非刚需开始日期，非刚需累积消费金额）

5.分别对表X/Y/Z 做分布，取会员数最多的区间段作为最终结果


STSC_DATE,MEMBER_ID,GOODS_CODE,GOODS_NAME,PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME,PROD_CATE_LEV2_CODE,PROD_CATE_LEV2_NAME,SELF_CATE_LEV2_NAME,SELF_CATE_LEV1_NAME,SALE_AMT,SALE_QTY


SELECT DISTINCT PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME FROM "EXT_TMP"."YBZ_MEMBER_ANY" 

INSTR('Y02',PROD_CATE)>0 or
INSTR('Y03',PROD_CATE)>0 or
INSTR('Y04',PROD_CATE)>0 or
INSTR('Y05',PROD_CATE)>0 or
INSTR('Y07',PROD_CATE)>0 or
INSTR('Y09',PROD_CATE)>0 or
INSTR('Y10',PROD_CATE)>0 or
INSTR('Y11',PROD_CATE)>0 or
INSTR('Y12',PROD_CATE)>0 or
INSTR('Y13',PROD_CATE)>0 



WITH  LC_MEMBER_B AS
(
	select
	STSC_DATE
	,MEMBER_ID
	,case when  
     PROD_CATE <> 'Y01' 	then 1 else 0 end as G_YES    --0是购买的刚需  1是购买的非刚需
	,SUM(SALE_AMT_SUM) OVER(PARTITION BY MEMBER_ID ORDER BY STSC_DATE ) AS SALE_AMT_SUM 
	,SALE_QTY_SUM
	,ROW_NUMBER()OVER(partition by MEMBER_ID order by STSC_DATE ) BUY_COUNT
	from (
		select
		STSC_DATE
		,MEMBER_ID
		,MAX(PROD_CATE_LEV1_CODE) as PROD_CATE 
		,SUM(SALE_AMT) SALE_AMT_SUM		
		,SUM(SALE_QTY) SALE_QTY_SUM		
		from
		"EXT_TMP"."YBZ_MEMBER_ANY" 
		where PROD_CATE_LEV1_CODE IS NOT NULL  and PROD_CATE_LEV1_CODE not in('Y85','Y86')   and SALE_AMT >0 and  MEMBER_ID='1951038'
		group by MEMBER_ID,STSC_DATE
	)
)


,

t2 as(

SELECT 
		b.MEMBER_ID
		,FLOOR(b.SALE_AMT_SUM/10)*10  SALE_AMT_SUM
		FROM LC_MEMBER_B b
		inner join (
		SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
		) c
		on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE
		WHERE B.BUY_COUNT>1 
	
)
,
t3 as(
select * from t2 where SALE_AMT_SUM=0
)

select * from t3 limit 100




select 
	SALE_AMT_SUM,
	count(1) ,count
	from 
	(
		SELECT 
		b.MEMBER_ID
		,FLOOR(b.SALE_AMT_SUM/10)*10  SALE_AMT_SUM
		FROM LC_MEMBER_B b
		inner join (
		SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
		) c
		on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE
		WHERE B.BUY_COUNT>1 
	)
	group by SALE_AMT_SUM
	order by SALE_AMT_SUM
	
	
	
	
	
	
	
	
	
	
	
	
	
	


SELECT 
,PERCENTILE_DISC (0.30) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_30
,PERCENTILE_DISC (0.40) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_40
,PERCENTILE_DISC (0.50) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_50
,PERCENTILE_DISC (0.60) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_60
,PERCENTILE_DISC (0.70) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_70
,PERCENTILE_DISC (0.80) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_80
,PERCENTILE_DISC (0.90) WITHIN GROUP (ORDER BY SALE_AMT_SUM ASC) over (partition by 1) as percent_90
FROM LC_MEMBER_B b
inner join (
SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
) c
on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE
WHERE B.BUY_COUNT>2 




-----------------------------

SELECT COUNT(1) FROM LC_MEMBER_B b
inner join (
SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
) c
on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE


SELECT COUNT(1) FROM LC_MEMBER_B b
inner join (
SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
) c
on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE
WHERE B.BUY_COUNT=1 

总共买了非刚需的人数：   				123,483
第一次就购买非刚需： 					107,259
一次以上购买非刚需：					 16,224


 LIMIT 3



PROD_CATE_LEV1_CODE
Y01
Y02
Y03
Y04
Y05
Y07
Y09
Y10
Y11
Y12
Y13
Y85
Y86

	 
	 
	 
	 
	 
	 
	 -------------------------最终逻辑 ----------------
	 
	 
	 
WITH  LC_MEMBER_B AS
(
	select
	STSC_DATE
	,MEMBER_ID
	,case when  
     PROD_CATE <> 'Y01' 	then 1 else 0 end as G_YES    --0是购买的刚需  1是购买的非刚需
	,SUM(SALE_AMT_SUM) OVER(PARTITION BY MEMBER_ID ORDER BY STSC_DATE ) AS SALE_AMT_SUM 
	,SALE_QTY_SUM
	,ROW_NUMBER()OVER(partition by MEMBER_ID order by STSC_DATE ) BUY_COUNT
	from (
		select
		STSC_DATE
		,MEMBER_ID
		,MAX(PROD_CATE_LEV1_CODE) as PROD_CATE 
		,SUM(SALE_AMT) SALE_AMT_SUM		
		,SUM(SALE_QTY) SALE_QTY_SUM		
		from
		"EXT_TMP"."YBZ_MEMBER_ANY" 
		where PROD_CATE_LEV1_CODE IS NOT NULL  and PROD_CATE_LEV1_CODE not in('Y85','Y86')   and SALE_AMT >0 
		group by MEMBER_ID,STSC_DATE
	)
)


select 
	SALE_AMT_SUM,
	count(1)  
	from 
	(
		SELECT 
		b.MEMBER_ID
		,FLOOR(b.SALE_AMT_SUM/10)*10  SALE_AMT_SUM
		FROM LC_MEMBER_B b
		inner join (
		SELECT MEMBER_ID,min(STSC_DATE) MIN_STSC_DATE FROM LC_MEMBER_B  where G_YES = 1 group by MEMBER_ID 
		) c
		on b.MEMBER_ID=c.MEMBER_ID and B.STSC_DATE=C.MIN_STSC_DATE
		WHERE B.BUY_COUNT>1 
	)
	group by SALE_AMT_SUM
	order by SALE_AMT_SUM
	
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 
	 