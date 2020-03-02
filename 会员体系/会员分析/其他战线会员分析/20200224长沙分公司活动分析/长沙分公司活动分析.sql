--时间：20200224
--贡献者：姚泊彰
--目的：对长沙分公司活动进行事前分析及为活动效果、内容提供数据支持与参考

--总共分为四步：
--1、得到总公司（长沙分公司）近半年每月1-18号消费后，19-25号还消费的情况
--首先，得到订单
with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 t."SALE_ORDR_DOC",  							--销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",     							--门店编码
				 t."GOODS_CODE",    							--商品编码
				 t."MEMBER_ID",								--会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME,
				 s.ADMS_ORG_CODE,
				 s.PHMC_S_NAME,
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
				 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
				 to_char(t."STSC_DATE",'DD') as AT_DAY,		--月中天数
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20200225'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20190101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			inner join (
				select 
					phmc_code,
					PHMC_S_NAME,
					ADMS_ORG_CODE,
					ADMS_ORG_NAME,
					phmc_type
				from dw.dim_phmc
			) s
			on s.phmc_code=t.phmc_code
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 s.ADMS_ORG_CODE,
				 s.PHMC_S_NAME,
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME
		)

--得到每个月哪些会员购买了，1-18号打个标，19-25号打个标
,
t2 as (
	select AT_MONTH
		,sum(buy_1_18_flag) as buy_1_18_membnum
		,sum(case when buy_1_18_flag =1 and buy_19_25_flag=1 then 1 else 0 end) as buy_both_membnum
	from
	(
		select AT_MONTH
			,MEMBER_ID
			,max(buy_1_18_flag) as buy_1_18_flag
			,max(buy_19_25_flag) as buy_19_25_flag
		from
		(
			select AT_MONTH
				,case when AT_DAY>=1 and AT_DAY<=18 then 1 else 0 end as buy_1_18_flag
				,case when AT_DAY>=19 and AT_DAY<=25 then 1 else 0 end as buy_19_25_flag
				,MEMBER_ID
			from t1
			where is_member='Y'
			and PHMC_S_NAME like '%岳阳%'
			--and ADMS_ORG_CODE='100B'			--这部分条件注释掉即是全公司情况
		)
		group by AT_MONTH
			,MEMBER_ID
	)
	group by AT_MONTH
		
)
--select * from t2 order by AT_MONTH ASC

--2、给出总公司（分公司）订单数、订单金额、消费会员数曲线图，给出长沙分公司会员销售占比
,
t3 as (
	SELECT SALE_AMT_FLOOR
		,SALE_NUM/SALE_NUM_TOTAL AS SALE_NUM_RATE
		,MEMB_NUM/MEMB_NUM_TOTAL AS MEMB_NUM_RATE
		,SALE_AMT/SALE_AMT_TOTAL AS SALE_AMT_RATE
	FROM
	(
		SELECT SALE_AMT_FLOOR
			,SALE_NUM
			,MEMB_NUM
			,SALE_AMT
			,SUM(SALE_NUM) OVER() AS SALE_NUM_TOTAL
			,SUM(MEMB_NUM) OVER() AS MEMB_NUM_TOTAL
			,SUM(SALE_AMT) OVER() AS SALE_AMT_TOTAL
		FROM
		(
			SELECT SALE_AMT_FLOOR
				,COUNT(1) AS SALE_NUM
				,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM
				,SUM(SALE_AMT) AS SALE_AMT
			FROM
			(
				select SALE_ORDR_DOC
					,sum(SALE_AMT) as SALE_AMT
					,floor(sum(SALE_AMT)/10)*10 as SALE_AMT_FLOOR
					,MAX(MEMBER_ID) AS MEMBER_ID
				from t1
				WHERE MEMBER_ID IS NOT NULL
					and PHMC_S_NAME like '%岳阳%'
					--and ADMS_ORG_CODE='100B'
				group by SALE_ORDR_DOC
			)
			WHERE SALE_AMT>0 --AND SALE_AMT<3000
			GROUP BY SALE_AMT_FLOOR
		)
	)
)
,
t3_1_1 as (
	select SALE_ORDR_DOC
		,PROD_CATE_LEV2_NAME
		,sum(SALE_AMT) as SALE_AMT
		--,floor(sum(SALE_AMT)/10)*10 as SALE_AMT_FLOOR
		,MAX(MEMBER_ID) AS MEMBER_ID
	from t1
	WHERE MEMBER_ID IS NOT NULL
		and PHMC_S_NAME like '%岳阳%'
		--and ADMS_ORG_CODE='100B'
	group by SALE_ORDR_DOC
		,PROD_CATE_LEV2_NAME
)
,
t3_1_2 as (
	select SALE_ORDR_DOC			--订单号
		,PROD_CATE_LEV2_NAME		--二级品类名称
		,floor(sum(SALE_AMT) over(partition by SALE_ORDR_DOC)/10)*10 as SALE_AMT_FLOOR
		,SALE_AMT					--销售金额
		,MEMBER_ID					--会员号
	from t3_1_1
	WHERE SALE_AMT>0

)
--select * from t3_1_2 limit 10
,

--找到每个客单区间的二级品类客单及单数
t3_1 as (
	select SALE_AMT_FLAG
		,PROD_CATE_LEV2_NAME
		,MEMB_NUM
		,SALE_AMT/SALE_NUM AS CATE_AVG_SALE
	FROM
	(
		SELECT SALE_AMT_FLAG
			,PROD_CATE_LEV2_NAME
			,COUNT(1) AS SALE_NUM
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM
			,SUM(SALE_AMT) AS SALE_AMT
		FROM
		(
			select SALE_ORDR_DOC			--订单号
				,PROD_CATE_LEV2_NAME		--二级品类名称
				,case when SALE_AMT_FLOOR <20 then 1
				 when SALE_AMT_FLOOR >=30 and SALE_AMT_FLOOR <50 then 2
				 when SALE_AMT_FLOOR >=70 and SALE_AMT_FLOOR <90 then 3
				 when SALE_AMT_FLOOR >=110 and SALE_AMT_FLOOR <130 then 4
				 when SALE_AMT_FLOOR >=160 and SALE_AMT_FLOOR <190 then 5
				 when SALE_AMT_FLOOR >=260 and SALE_AMT_FLOOR <290 then 6
				 when SALE_AMT_FLOOR >=360 and SALE_AMT_FLOOR <390 then 7
				 else 8 end as SALE_AMT_FLAG
				,SALE_AMT					--销售金额
				,MEMBER_ID					--会员号
			from t3_1_2
		)
		GROUP BY SALE_AMT_FLAG
			,PROD_CATE_LEV2_NAME
	)
	
)
--SELECT * FROM T3 where SALE_AMT_FLOOR<1000  order by SALE_AMT_FLOOR asc
SELECT * FROM T3_1 order by SALE_AMT_FLAG,MEMB_NUM DESC

--3、找出每个人近半年买过三次的商品及每个会员最低购买价格
--找出这类商品买的最多的会员数
--找出每个商品在会员上80%的分位点
,
t4 as (
	SELECT GOODS_CODE
		,MAX(GOODS_NAME) AS GOODS_NAME
		,MAX(GOODS_amt_percent_80) AS GOODS_amt_percent_80
		,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM
	FROM
	(
		SELECT GOODS_CODE
			,GOODS_NAME
			,MEMBER_ID
			,PERCENTILE_DISC (0.2) WITHIN GROUP ( ORDER BY MIN_GOODS_AMT ASC) over(PARTITION BY GOODS_CODE) as GOODS_amt_percent_80	
		FROM
		(
			SELECT MEMBER_ID
				,GOODS_CODE
				,MAX(GOODS_NAME) AS GOODS_NAME
				,COUNT(DISTINCT SALE_ORDR_DOC) AS BUY_NUM
				,MIN(SALE_AMT/SALE_QTY) AS MIN_GOODS_AMT
			FROM t1
			WHERE MEMBER_ID IS NOT NULL
			AND SALE_QTY>0
			and PHMC_S_NAME like '%岳阳%'
			group by MEMBER_ID
				,GOODS_CODE
		)
		WHERE BUY_NUM>=3
	)
	GROUP BY GOODS_CODE
)
--SELECT * from t4 order by MEMB_NUM desc limit 500


--4、找到近一年所有没有使用促销方案的订单
--找到这些订单里面购买人数最高、频次最高的商品前100
,
t5 as (
	SELECT GOODS_CODE
		,GOODS_NAME
		,MEMB_NUM
		,CASE WHEN MEMB_NUM!=0 THEN ORDR_NUM/MEMB_NUM ELSE 0 END AS MEMB_ORDR_NUM
	FROM
	(
		SELECT GOODS_CODE
			,MAX(GOODS_NAME) AS GOODS_NAME
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM
			,COUNT(DISTINCT SALE_ORDR_DOC) AS ORDR_NUM
		FROM
		(
			SELECT MEMBER_ID
				,GOODS_CODE
				,GOODS_NAME
				,SALE_ORDR_DOC
			FROM 
			WHERE MEMBER_ID IS NOT NULL
			AND PRMN_PROG_CODE IS NULL
			--and PHMC_S_NAME like '%岳阳%'
			
		)
		GROUP BY GOODS_CODE
	)
	
)
SELECT * FROM T5 ORDER BY MEMB_NUM DESC LIMIT 500








