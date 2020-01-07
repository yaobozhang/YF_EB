create column table "EXT_TMP"."YBZ_MEMBER_ANY1" as
(
	select s.stsc_date,					--日期
		s.member_id,					--会员ID
		s.GOODS_CODE,
		s.GOODS_NAME,
		s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
		s.PROD_CATE_LEV1_NAME,
		s.PROD_CATE_LEV2_CODE,
		s.PROD_CATE_LEV2_NAME,
		s.PROD_CATE_LEV3_CODE,
		s.PROD_CATE_LEV3_NAME,
		s.PROD_CATE_LEV4_CODE,
		s.PROD_CATE_LEV4_NAME,
		CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
		  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
		  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV2_NAME
		,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
		  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
		  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV1_NAME
		,SALE_AMT
		,SALE_QTY
	from (
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."GOODS_CODE",    							--商品编码
			 t."MEMBER_ID",								--会员编码
			 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			 g.PROD_CATE_LEV1_NAME,
			 g.PROD_CATE_LEV2_CODE,
			 g.PROD_CATE_LEV2_NAME,
			 g.PROD_CATE_LEV3_CODE,
			 g.PROD_CATE_LEV3_NAME,
			 g.PROD_CATE_LEV4_CODE,
			 g.PROD_CATE_LEV4_NAME,
			 g.GOODS_NAME,
			 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
			 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20190101'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."GOODS_CODE",                                 --商品编码
			 t."MEMBER_ID",									 --会员编码
			 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			 g.PROD_CATE_LEV1_NAME,
			 g.PROD_CATE_LEV2_CODE,
			 g.PROD_CATE_LEV2_NAME,
			 g.PROD_CATE_LEV3_CODE,
			 g.PROD_CATE_LEV3_NAME,
			 g.PROD_CATE_LEV4_CODE,
			 g.PROD_CATE_LEV4_NAME,
			 g.GOODS_NAME
		) s
	left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
	on S.GOODS_CODE = T.GOODS_CODE
)





insert into "EXT_TMP"."YBZ_MEMBER_ANY"
(
	stsc_date,					--日期
	member_id,					--会员ID
	GOODS_CODE,
	GOODS_NAME,
	PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
	PROD_CATE_LEV1_NAME,
	PROD_CATE_LEV2_CODE,
	PROD_CATE_LEV2_NAME,
	PROD_CATE_LEV3_CODE,
	PROD_CATE_LEV3_NAME,
	PROD_CATE_LEV4_CODE,
	PROD_CATE_LEV4_NAME,
	SELF_CATE_LEV2_NAME,
	SELF_CATE_LEV1_NAME,
	SALE_AMT,
	SALE_QTY
)
	select s.stsc_date,					--日期
		s.member_id,					--会员ID
		s.GOODS_CODE,
		s.GOODS_NAME,
		s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
		s.PROD_CATE_LEV1_NAME,
		s.PROD_CATE_LEV2_CODE,
		s.PROD_CATE_LEV2_NAME,
		s.PROD_CATE_LEV3_CODE,
		s.PROD_CATE_LEV3_NAME,
		s.PROD_CATE_LEV4_CODE,
		s.PROD_CATE_LEV4_NAME,
		CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
		  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
		  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV2_NAME
		,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
		  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
		  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV1_NAME
		,SALE_AMT
		,SALE_QTY
	from (
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."GOODS_CODE",    							--商品编码
			 t."MEMBER_ID",								--会员编码
			 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			 g.PROD_CATE_LEV1_NAME,
			 g.PROD_CATE_LEV2_CODE,
			 g.PROD_CATE_LEV2_NAME,
			 g.PROD_CATE_LEV3_CODE,
			 g.PROD_CATE_LEV3_NAME,
			 g.PROD_CATE_LEV4_CODE,
			 g.PROD_CATE_LEV4_NAME,
			 g.GOODS_NAME,
			 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
			 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20200101'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20190101')) t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."GOODS_CODE",                                 --商品编码
			 t."MEMBER_ID",									 --会员编码
			 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			 g.PROD_CATE_LEV1_NAME,
			 g.PROD_CATE_LEV2_CODE,
			 g.PROD_CATE_LEV2_NAME,
			 g.PROD_CATE_LEV3_CODE,
			 g.PROD_CATE_LEV3_NAME,
			 g.PROD_CATE_LEV4_CODE,
			 g.PROD_CATE_LEV4_NAME,
			 g.GOODS_NAME
		) s
	left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
	on S.GOODS_CODE = T.GOODS_CODE



