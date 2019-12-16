--门店销售下滑分析（会员视角）
--代码贡献者：姚泊彰
--代码更新时间：20191105
--数据口径：见各自模块


--简介：门店销售下滑分析（会员视角）总共有几个步骤：1、销售现状趋势；2、会员因素定位；3、因素拆解

--0、数据准备
	--0.1、统一口径-订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋））；只拿宁乡和浏阳的门店；时间范围看三年；
		with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 case when weekday("STSC_DATE")+1>=1 and weekday("STSC_DATE")+1<=5 then 'Y' ELSE 'N'  END as is_weekday,
				 t."SALE_ORDR_DOC",  							--销售订单号
                 t."ORDR_SALE_TIME", 							--销售时间
				 t."PHMC_CODE",     							--门店编码
				 case when g1.PROP_ATTR in ('Z02','Z07') THEN '2'		--收购
				WHEN g1.PROP_ATTR in ('Z03','Z04') THEN '3' 	--加盟
				when g1.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --直营
				else 0
				end as PHMC_TYPE,			--门店类型
				g1.PHMC_S_NAME,			--门店简称
				case when g2.PHMC_CODE is not null then 1 else 0 end as ANY_PHMC,	--待分析门店
				 t."GOODS_CODE",    							--商品编码
				 t."MEMBER_ID",								--会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME,
				 --to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
				 to_char(t."STSC_DATE",'YYYYMM') as AT_TEARMONTH,		--年份+月份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
				,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20161101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			inner join dw.DIM_PHMC g1 on t.PHMC_CODE=g1.PHMC_CODE
			left join "EXT_TMP"."BOZHANG_PHMC_DESC_ANY" g2		--拿出待分析门店
			on g2.PHMC_CODE = t.PHMC_CODE 
			where g1.dist_code in ('10011434','20013762','10012924','10017063')  --浏阳一区，浏阳二区，宁乡一区，宁乡二区
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME,
				 case when g1.PROP_ATTR in ('Z02','Z07') THEN '2'		--收购
				WHEN g1.PROP_ATTR in ('Z03','Z04') THEN '3' 	--加盟
				when g1.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --直营
				else 0
				end ,
				PHMC_S_NAME ,
				case when g2.PHMC_CODE is not null then 1 else 0 end
		)
	,
		--继续关联，同人同天同门店算一次
		t1_1 as (
			select 
			s.stsc_date,
			s.is_weekday,
			s.member_id,
			s.phmc_code,
			s.sale_amt,
			s.GROS_PROF_AMT,
			s.PURC_MONEY, 	--营销销售
			s.NO_PURC_MONEY,	--非营销
			s.GOODS_CODE,
			s.GOODS_NAME,
			c.birthday,
			c.sex,
			C.create_time,
			c.come_from,
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
			from t1 s
			left join  
			(
				select 
				customer_id,
				birthday,
				sex,
				create_time,
				come_from
				from ds_crm.tp_cu_customerbase
			) c 
			on s.member_id=c.customer_id
			where ANY_PHMC=1
		)
	,

/*--1、门店销售现状趋势*/
	--1.1、得到范围内门店3年到月销售数据
	t2 as (
		select PHMC_CODE					--门店编码
			,AT_TEARMONTH					--年月
			,sum(SALE_AMT) as SALE_AMT		--销售额
			,sum(case when is_member='Y' then SALE_AMT else 0 end) as memb_sale_amt
			,max(PHMC_S_NAME) AS PHMC_NAME
		from t1 
		group by PHMC_CODE,AT_TEARMONTH
	)	
	select PHMC_CODE,AT_TEARMONTH,SALE_AMT,memb_sale_amt,PHMC_NAME from t2 order by PHMC_CODE,AT_TEARMONTH asc
       