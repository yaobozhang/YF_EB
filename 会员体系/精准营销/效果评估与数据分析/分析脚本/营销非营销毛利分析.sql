--开发者：姚泊彰
--问题：分析所有用券订单中营销品种和非营销品种的毛利情况
--注意点：1、全场用券看用券整单，品类用券看与券品类相同品类商品，单品用券看用券商品
--是否需要修改参数：否
--开发时间：20190107
--修改记录
--


--查看单品券、品类券、全场券精准营销营销品种和非营销品种毛利率
--STEP1：找到所有用券整单详细信息
with t1 as
(
select t1.TEMPLATE_ID		--券模板号
	,t1.GOODS_CODE AS BUY_GOODS_CODE				--购买商品
	,t1.sale_money					--销售额
	,t1.GROS_PROF_AMT		--毛利额
	,t1.SINGLEGOODSCODE			--券模板单品
	,t1.COUPONTYPE				--券模板类型
	,t1.group_cd				--券模板品类CD
	,t2.group_cd as buy_group_cd
	,t2.PURC_CLAS_LEV1_NAME		--是否营销
from
(
	SELECT t1.TEMPLATE_ID		--券模板号
	,t2.GOODS_CODE				--购买商品
	,t2.COST_AMT+t2.GROS_PROF_AMT	as sale_money					--销售额
	,t2.GROS_PROF_AMT		--毛利额
	,t3.SINGLEGOODSCODE			--券模板单品
	,t3.COUPONTYPE				--券模板类型
	,t3.group_cd				--券模板品类CD
	FROM
	"DS_CRM"."ZT_COUPON" t1
	inner join 
	"DW"."FACT_SALE_ORDR_DETL_LXX" t2
	on t1.ORDER_CODE=t2.SALE_ORDR_DOC
	inner join
	(
		SELECT t1.code,t1.COUPONTYPE,t1.SINGLEGOODSCODE,t2.group_cd
		FROM
		"DW"."BI_TEMP_COUPON_ALL" t1
		LEFT JOIN
		"DW"."CONFIG_CATEGORY_TYPE_LABEL" t2
		on LEFT(t1.CLASSGOODSLIST,5)=t2.cat_cd	
	) t3
	on t1.TEMPLATE_ID=t3.code
) t1
LEFT JOIN
"DW"."DIM_GOODS_SALE_CATEGORY" t2
on t1.GOODS_CODE=t2.goods_code
)
,
--STEP2：统计单品数据
t2 as
(
	SELECT PURC_CLAS_LEV1_NAME
	,SUM(sale_money) AS SALE_TOTAL					--销售额
	,SUM(GROS_PROF_AMT) AS GROS_TOTAL		--毛利额
	,SUM(GROS_PROF_AMT)/SUM(sale_money) AS GROS_RATE--毛利率
	FROM T1
	WHERE COUPONTYPE='ITEM'
	AND BUY_GOODS_CODE=SINGLEGOODSCODE
	GROUP BY PURC_CLAS_LEV1_NAME
)
,
--STEP3：统计品类数据
t3 as
(
	SELECT PURC_CLAS_LEV1_NAME
	,SUM(sale_money) AS SALE_TOTAL					--销售额
	,SUM(GROS_PROF_AMT) AS GROS_TOTAL		--毛利额
	,SUM(GROS_PROF_AMT)/SUM(sale_money) AS GROS_RATE--毛利率
	FROM T1
	WHERE COUPONTYPE='GOODS'
	AND BUY_group_cd=group_cd
	GROUP BY PURC_CLAS_LEV1_NAME
)
,
--STEP4：统计全场数据
t4 as
(
	SELECT PURC_CLAS_LEV1_NAME
	,SUM(sale_money) AS SALE_TOTAL					--销售额
	,SUM(GROS_PROF_AMT) AS GROS_TOTAL		--毛利额
	,SUM(GROS_PROF_AMT)/SUM(sale_money) AS GROS_RATE--毛利率
	FROM T1
	WHERE COUPONTYPE='ALL'
	GROUP BY PURC_CLAS_LEV1_NAME
)
,
--STEP5：全部数据
t5 as
(
	SELECT '单品' as coupontype,PURC_CLAS_LEV1_NAME,SALE_TOTAL,GROS_TOTAL,GROS_RATE FROM T2
	UNION ALL
	SELECT '品类' as coupontype,PURC_CLAS_LEV1_NAME,SALE_TOTAL,GROS_TOTAL,GROS_RATE FROM T3
	UNION ALL
	SELECT '全场' as coupontype,PURC_CLAS_LEV1_NAME,SALE_TOTAL,GROS_TOTAL,GROS_RATE FROM T4
)
,
--STEP6：求和
t6 as
(
	SELECT '总体' as coupontype,PURC_CLAS_LEV1_NAME
	,SUM(SALE_TOTAL) AS SALE_TOTAL
	,SUM(GROS_TOTAL) AS GROS_TOTAL
	,SUM(GROS_TOTAL)/SUM(SALE_TOTAL) AS GROS_RATE
	FROM
	t5
	GROUP BY PURC_CLAS_LEV1_NAME
)
SELECT coupontype	--券类型
	,PURC_CLAS_LEV1_NAME	--是否营销
	,SALE_TOTAL	--总销售额
	,GROS_TOTAL	--总毛利额
	,GROS_RATE --总毛利率
FROM t6






