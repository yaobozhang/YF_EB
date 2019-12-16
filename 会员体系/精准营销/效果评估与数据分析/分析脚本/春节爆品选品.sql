-- step1：全品类销售等级表
	drop table EXT_TMP.hot_goods_spring;
	create column table  EXT_TMP.hot_goods_spring as 
	(
	select 	goods_code,
			GOODS_NAME,
			PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME,
			PROD_CATE_LEV2_CODE,PROD_CATE_LEV2_NAME,
			PROD_CATE_LEV3_CODE,PROD_CATE_LEV3_NAME ,
			PROD_CATE_LEV4_CODE,PROD_CATE_LEV4_NAME,
			PURC_CLAS_LEV3_CODE,PURC_CLAS_LEV3_NAME,-- 采购分类
			SALE_QTY,  -- 期间内销售数量
			SALE_AMT,  -- 期间内销售金额
			GROS_PROF, -- 期间内销售毛利额
			AVG_GROS_PCT, -- 平均毛利率
			AVG_MEMB_PRIC, -- 会员价
			AVG_GROS_PRO, -- 平均毛利额
			AVG_COST_PRIC,  --成本价
			ROW_NUMBER()OVER(partition by PROD_CATE_LEV1_CODE ORDER BY SALE_QTY DESC) AS QTY_CNT,-- 销售数量排名
			ROW_NUMBER()OVER(partition by PROD_CATE_LEV1_CODE ORDER BY SALE_AMT DESC) AS AMT_CNT,-- 销售额排名
			ROW_NUMBER()OVER(partition by PROD_CATE_LEV1_CODE ORDER BY GROS_PROF DESC) AS GROS_CNT,-- 毛利额排名
			round(SALE_QTY/(SUM(SALE_QTY)OVER(partition by PROD_CATE_LEV1_CODE)),3) AS SALE_QTY_PCT, --销量贡献率
			round(SALE_AMT/(SUM(SALE_AMT)OVER(partition by PROD_CATE_LEV1_CODE)),3) AS SALE_AMT_PCT, --销售额贡献率
			round(GROS_PROF/(SUM(GROS_PROF)OVER(partition by PROD_CATE_LEV1_CODE)),3) AS GROS_PROF_PCT -- 毛利额贡献率	
		from 
			(select a.goods_code,
					GOODS_NAME,
					PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME,
					PROD_CATE_LEV2_CODE,PROD_CATE_LEV2_NAME,
					PROD_CATE_LEV3_CODE,PROD_CATE_LEV3_NAME,
					PROD_CATE_LEV4_CODE,PROD_CATE_LEV4_NAME,
					PURC_CLAS_LEV3_CODE,PURC_CLAS_LEV3_NAME,
					SALE_QTY,
					SALE_AMT,
					GROS_PROF,
					ROUND((MEMB_PRIC/SALE_QTY),3) AS AVG_MEMB_PRIC,
					ROUND((COST_PRIC/SALE_QTY),3) AS AVG_COST_PRIC,
					ROUND(GROS_PROF/SALE_QTY,3) AS AVG_GROS_PRO,
					ROUND((GROS_PROF/SALE_AMT)/SALE_QTY,3) AS AVG_GROS_PCT
				from 
				(
				(select goods_code,
					SUM(MEMB_PRIC) AS MEMB_PRIC,
					SUM(COST_PRIC) AS COST_PRIC,
					SUM(SALE_QTY) AS SALE_QTY,
					SUM(SALE_AMT) AS SALE_AMT,
					SUM(SALE_AMT-COST_PRIC*SALE_QTY) AS GROS_PROF
				FROM DW.FACT_SALE_ORDR_DETL
				WHERE ORDR_SALE_TIME>=ADD_DAYS(TO_DATE('20180215'),-15) and ORDR_SALE_TIME<ADD_DAYS(TO_DATE('20180215'),0) AND SALE_QTY>0 AND SALE_AMT>0
				GROUP BY GOODS_CODE)a
				LEFT JOIN 
				(SELECT GOODS_CODE,GOODS_NAME,
					PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME,
					PROD_CATE_LEV2_CODE,PROD_CATE_LEV2_NAME,
					PROD_CATE_LEV3_CODE,PROD_CATE_LEV3_NAME,
					PROD_CATE_LEV4_CODE,PROD_CATE_LEV4_NAME,
					PURC_CLAS_LEV3_CODE,PURC_CLAS_LEV3_NAME
				FROM DW.DIM_GOODS_SALE_CATEGORY)b
				on a.goods_code=b.goods_code
				)
			)
		WHERE AVG_GROS_PCT>0.5
		)
-- step2：筛选所需品类销售
select  a.goods_code,GOODS_NAME,
		PROD_CATE_LEV1_CODE,PROD_CATE_LEV1_NAME,
		PROD_CATE_LEV2_CODE,PROD_CATE_LEV2_NAME,
		PROD_CATE_LEV3_CODE,PROD_CATE_LEV3_NAME,
		PROD_CATE_LEV4_CODE,PROD_CATE_LEV4_NAME,
		PURC_CLAS_LEV3_CODE,PURC_CLAS_LEV3_NAME,-- 采购分类
		SALE_QTY,  -- 期间内销售数量
		SALE_AMT,  -- 期间内销售金额
		GROS_PROF, -- 期间内销售毛利额
		bill_num, -- 期间订单数量
		AVG_GROS_PCT, -- 毛利率
		AVG_MEMB_PRIC, --会员价
		AVG_GROS_PRO, -- 平均毛利额
		AVG_COST_PRIC, -- 成本价
		store_quantity,  --现有库存
		memb_num, --购买会员人数
		goods_memb_num, --会员购买数量
		round(goods_memb_num/memb_num,3) as num_per_memb, --会员平均购买件数
		ROUND(SALE_QTY/bill_num,2) AS NUM_PER_BILL -- 每单购买件数
from 
(
	(select * from  EXT_TMP.hot_goods_spring 
		where PROD_CATE_LEV2_CODE='Y0107'or PROD_CATE_LEV2_CODE='Y0108'or PROD_CATE_LEV2_CODE='Y0113'
		or PROD_CATE_LEV2_CODE='Y0112' or PROD_CATE_LEV2_CODE='Y0116'or PROD_CATE_LEV2_CODE='Y0109'or PROD_CATE_LEV2_CODE='Y0110'
		or PROD_CATE_LEV2_CODE='Y0901'or (PROD_CATE_LEV2_CODE='Y0202' and PROD_CATE_LEV4_NAME not like '散装类' ) 
		or PROD_CATE_LEV1_CODE='Y03')a
left join 
	(select good_code,sum(STOCK_QTY) as store_quantity 
			from DS_POS.BASE_GOODSSTORE --拿近一个月库存 
			where 
			loadtime>=add_days(to_date('20190110'),-30) 
			and loadtime<=add_days(to_date('20190110'),0)
			group by good_code)b
on a.goods_code=b.good_code
left join 
(-- 会员购买人数、会员人均购买件数
	select 	A.goods_code,
			COUNT(DISTINCT MEMB_CODE) as memb_num,  --购买会员人数
			sum(goods_bill_num) as goods_memb_num -- 会员购买件数
	from 
		((select goods_code,
				SALE_ORDR_DOC,
				count(goods_code) as goods_bill_num -- 每单购买件数
			from DW.FACT_SALE_ORDR_DETL
			where ORDR_SALE_TIME>=ADD_DAYS(TO_DATE('20180215'),-15) and ORDR_SALE_TIME<ADD_DAYS(TO_DATE('20180215'),0) AND SALE_QTY>0 AND SALE_AMT>0
			group by goods_code,SALE_ORDR_DOC)a
		left join 
		(select SALE_ORDR_DOC,MEMB_CODE from DW.FACT_SALE_ORDR)b
		on a.SALE_ORDR_DOC=b.SALE_ORDR_DOC
			) 
		where MEMB_CODE is not null and MEMB_CODE<>''
		group by a.goods_code)c
on a.goods_code=c.goods_code
LEFT JOIN 
(SELECT GOODS_CODE,COUNT(SALE_ORDR_DOC) AS BILL_NUM --订单数 
	FROM 
		(
		select goods_code,
			   SALE_ORDR_DOC,
			   count(goods_code) as goods_bill_num -- 每单购买件数
		from DW.FACT_SALE_ORDR_DETL
		where ORDR_SALE_TIME>=ADD_DAYS(TO_DATE('20180215'),-15) and ORDR_SALE_TIME<ADD_DAYS(TO_DATE('20180215'),0) AND SALE_QTY>0 AND SALE_AMT>0
		GROUP BY GOODS_CODE,SALE_ORDR_DOC
		)
	group by goods_code
)d
ON A.GOODS_CODE=D.GOODS_CODE
);
	


	
	
	
	











	
	
	
	