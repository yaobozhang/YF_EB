	with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 t."SALE_ORDR_DOC",  							--销售订单号
				 t."PHMC_CODE",     							--门店编码
				 t."GOODS_CODE",    							--商品编码
				 t."MEMBER_ID",								--会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
				 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum( case when g.PURC_CLAS_LEV1_CODE='01' then t.sale_amt end) as PURC_MONEY, 			--营销销售
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20150101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME
		)                    
		--2.3.1、取三年数据的口径，即t1，做同人同天同门店算一次处理，增加统计指标，得到营销销售额，所属品类
	,t2 as (
		select
			stsc_date,					--日期
			member_id,					--会员ID
			phmc_code,					--门店号
			AT_TEAR,					--年份带出来
			sum(sale_amt) as sale_amt, 	--销售额
			sum(PURC_MONEY) as PURC_MONEY,--营销销售额
			sum(GROS_PROF_AMT) as gros	--毛利额
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			phmc_code,
			AT_TEAR
	)
	,
	--品类专用
	t2_1 as (
		--得到拼接后的品类
		select member_id
			,AT_TEAR
			,PROD_CATE_LEV2_NAME
			,sum(SALE_AMT) as SALE_AMT
			,count(1) as sale_times
		from
		(
			select	stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_TEAR,					--年份带出来
				PROD_CATE_LEV2_NAME,
				sum(SALE_AMT) as SALE_AMT
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_TEAR,					--年份带出来
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
				,SALE_AMT
				from t1 s
				left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
				on S.GOODS_CODE = T.GOODS_CODE
			)
			group by stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_TEAR,					--年份带出来
				PROD_CATE_LEV2_NAME
		)
		group by member_id
			,AT_TEAR
			,PROD_CATE_LEV2_NAME
	)
	,
	--到月专用
	t2_2 as (
		select
			stsc_date,					--日期
			member_id,					--会员ID
			phmc_code,					--门店号
			AT_TEAR,					--年份带出来
			AT_MONTH,					--月份带出来
			sum(sale_amt) as sale_amt, 	--销售额
			sum(PURC_MONEY) as PURC_MONEY,--营销销售额
			sum(GROS_PROF_AMT) as gros	--毛利额
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			phmc_code,
			AT_TEAR,
			AT_MONTH
	)
	,
	--每人每年进行统计
	t3_0 as
	(
		select 
				 AT_TEAR,
				 MEMBER_ID,
				 sum(SALE_AMT) as SALE_AMT,
				 count(1) as sale_times,
				 sum(PURC_MONEY) as PURC_MONEY--营销销售额
			 from t2 
			 group by AT_TEAR,
				MEMBER_ID
	)
	,
	--得到会员所属门店是否收购加盟、是否新老店、是哪个分公司
	--首先，得到会员所属门店
	t3_1 as
	(
		select t1.MEMBER_ID			--会员编码
			,t1.AT_STORE			--所属门店
			,t2.ADMS_ORG_CODE		--分公司编码
			,t2.ADMS_ORG_NAME		--分公司名称
			,t2.company_code		--公司编码='4000'
			,t2.PROP_ATTR			--财务字段 in ('Z02','Z07')
		from
		(
			select t1.MEMBER_ID			--会员编码
				,t2.AT_STORE			--所属门店
			from
			(
				select 
					MEMBER_ID	--会员编码
				from t3_0
				group by member_id
			)t1
			left join  
			(
				select 
					customer_id,
					AT_STORE
				from ds_crm.tp_cu_customerbase
			)t2
			on t1.member_id=t2.customer_id
		)t1
		left join dw.DIM_PHMC t2
		on t1.AT_STORE=t2.PHMC_CODE	
	)
	,
	--判断所属门店类型
	t3 as
	(
		select  t1.AT_TEAR
			,t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.PURC_MONEY	--营销销售额
			,t1.sale_times
			,case when left(t2.company_code,1)=4 or t2.PROP_ATTR in ('Z02','Z07') then 'SG_JM' else 'NORMAL' end as SG_JM_FLAG		--是否收购加盟
			,t2.ADMS_ORG_NAME		--分公司名称
		from t3_0 t1 
		left join t3_1 t2
		on t1.MEMBER_ID=t2.member_id
	)
	,
	--到月专用，关联得到会员公司及是否收购加盟
	t3_2_1 as
	(
		select t1.member_id,				--会员ID
			t1.AT_TEAR,					--年份带出来
			t1.AT_MONTH,					--月份带出来
			t1.sale_amt, 					--销售额
			t1.PURC_MONEY,					--营销销售额
			t1.gros,						--毛利额
			t1.sale_times,
			t2.SG_JM_FLAG,				--是否收购加盟
			t2.ADMS_ORG_NAME			--分公司名称
		from
		(
			select
				member_id,					--会员ID
				AT_TEAR,					--年份带出来
				AT_MONTH,					--月份带出来
				sum(sale_amt) as sale_amt, 	--销售额
				count(1) as sale_times,		--销售次数
				sum(PURC_MONEY) as PURC_MONEY,--营销销售额
				sum(GROS_PROF_AMT) as gros	--毛利额
			from t1
			group by 
				member_id,
				AT_TEAR,
				AT_MONTH
		)t1
		left join
		(
			select MEMBER_ID
				,max(SG_JM_FLAG) as SG_JM_FLAG		--是否收购加盟
				,max(ADMS_ORG_NAME) as ADMS_ORG_NAME		--分公司名称
			from t3
			group by member_id
		)t2
		on t1.member_id=t2.member_id
	)
	,
	--到月专用，关联年数据，得到每个月的复购标识
	t3_2 as
	(
		select t1.member_id,				--会员ID
			t1.AT_TEAR,						--年份带出来
			t1.AT_MONTH,					--月份带出来
			t1.sale_amt, 					--销售额
			t1.PURC_MONEY,					--营销销售额
			t1.sale_times,			
			t1.gros,							--毛利额
			t1.SG_JM_FLAG,					--是否收购加盟
			t1.ADMS_ORG_NAME,				--分公司名称
			case when t2.member_id is not null then 1 else 0 end as IS_CB_FLAG		--是否复购
		from t3_2_1 t1
		left join t3_0 t2 
		on t1.member_id=t2.member_id
		and t1.AT_TEAR=t2.AT_TEAR+1
		where t1.SG_JM_FLAG='NORMAL'		--只分析非收购加盟的
	)
	,
	--取各年度复购会员数、销售额、消费次数
	t4 as (
		select	AT_TEAR				--年份
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--复购营销金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--分公司名称
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
				 ,t1.PURC_MONEY	--营销销售额
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
		)
		group by AT_TEAR
	)
	,
	--计算各年度消费会员总数	  
	t5 as ( 
		select 
			AT_TEAR,
			count(1) as total_qty --消费会员总数	
		from t3
		group by AT_TEAR
	 )
	 ,
	 --得到各年度复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6 as 
	 (
		select 	 
			t5.AT_TEAR,					--年份
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times, 		--总消费次数
			t4.return_memb_sale_purc	--复购营销金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--人均复购营销金额
		from t5
		left join t4
		on t5.AT_TEAR=t4.AT_TEAR
	 )
	,
	--取各年度收购加盟复购会员数、销售额、消费次数
	t4_1 as (
		select	AT_TEAR				--年份
			,SG_JM_FLAG
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--分公司名称
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
		)
		group by AT_TEAR
		,SG_JM_FLAG
	)
	,
	--计算各年度消费会员总数	  
	t5_1 as ( 
		select 
			AT_TEAR,
			SG_JM_FLAG,
			count(1) as total_qty --消费会员总数	
		from t3
		group by AT_TEAR
		,SG_JM_FLAG
	 )
	 ,
	 --得到各年度是否收购加盟的复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_1 as 
	 (
		select 	 
			t5.AT_TEAR,					--年份
			t5.SG_JM_FLAG,				--是否收购加盟
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times 		--总消费次数
			--,t4.return_memb_sale/t4.return_memb_num as return_memb_avg_sale		--人均销售金额
			--,t4.return_memb_times/t4.return_memb_num as return_memb_avg_times	--人均销售次数
		from t5_1 t5
		left join t4_1 t4
		on t5.AT_TEAR=t4.AT_TEAR
		and t5.SG_JM_FLAG=t4.SG_JM_FLAG
	 )
	 ,
	--取各年度分公司复购会员数、销售额、消费次数
	t4_2 as (
		select	AT_TEAR				--年份
			,ADMS_ORG_NAME
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--分公司名称
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
			where t1.SG_JM_FLAG='NORMAL'		--只分析非收购加盟的
		)
		group by AT_TEAR
		,ADMS_ORG_NAME
	)
	,
	--计算各年度分公司消费会员总数	  
	t5_2 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			count(1) as total_qty --消费会员总数	
		from t3
		group by AT_TEAR
		,ADMS_ORG_NAME
	 )
	 ,
	 --得到各年度分公司复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_2 as 
	 (
		select 	 
			t5.AT_TEAR,					--年份
			t5.ADMS_ORG_NAME,				--是否收购加盟
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times 		--总消费次数
			--,t4.return_memb_sale/t4.return_memb_num as return_memb_avg_sale		--人均销售金额
			--,t4.return_memb_times/t4.return_memb_num as return_memb_avg_times	--人均销售次数
		from t5_2 t5
		left join t4_2 t4
		on t5.AT_TEAR=t4.AT_TEAR
		and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
	 )
	 ,
	--取各年度各品类复购会员数、销售额、消费次数
	t4_3 as (
		select	AT_TEAR				--年份
			,PROD_CATE_LEV2_NAME
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.PROD_CATE_LEV2_NAME,
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t2_1 t1
			left join t2_1 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
			and t1.PROD_CATE_LEV2_NAME=t2.PROD_CATE_LEV2_NAME
		)
		group by AT_TEAR
		,PROD_CATE_LEV2_NAME
	)
	,
	--计算各年度各品类消费会员总数	  
	t5_3 as ( 
		select 
			AT_TEAR,
			PROD_CATE_LEV2_NAME,
			count(1) as total_qty --消费会员总数	
		from t2_1
		group by AT_TEAR,
			PROD_CATE_LEV2_NAME
	 )
	 ,
	 --得到各年度复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_3 as 
	 (
		select 	 
			t5.AT_TEAR,					--年份
			T5.PROD_CATE_LEV2_NAME,
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times 		--总消费次数
		from t5_3 t5
		left join t4_3 t4
		on t5.AT_TEAR=t4.AT_TEAR
		AND T5.PROD_CATE_LEV2_NAME=T4.PROD_CATE_LEV2_NAME
	 )
	  ,
	--取各分公司月度复购会员数、销售额、消费次数
	t4_4 as (
		select	AT_TEAR							--月度
			,AT_MONTH
			,ADMS_ORG_NAME
			,sum(IS_CB_FLAG) as return_memb_num	--复购人数
			,sum(case when IS_CB_FLAG =1 then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when IS_CB_FLAG =1 then sale_times else 0 end) as return_memb_times	--复购次数
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.AT_MONTH,
				 t1.SG_JM_FLAG,
				 t1.IS_CB_FLAG,				--是否复购
				 t1.ADMS_ORG_NAME,			--分公司名称
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t3_2 t1
			
		)
		group by AT_TEAR
		,AT_MONTH
		,ADMS_ORG_NAME
	)
	,
	--计算各年年度分公司消费会员总数	  
	t5_4 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			count(distinct member_id) as total_qty --消费会员总数	
		from t3_2
		group by AT_TEAR
		,ADMS_ORG_NAME
	 )
	 ,
	 --得到各月度分公司复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_4 as 
	 (
		select 	
			t4.AT_TEAR,
			t4.AT_MONTH,
			t4.ADMS_ORG_NAME,				--分公司
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times, 		--总消费次数
			t5.total_qty				--总消费人数
		from t4_4 t4
		left join t5_4 t5
		on t4.AT_TEAR=t5.AT_TEAR+1
		and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
	 )
	 
	 select * from t6_2