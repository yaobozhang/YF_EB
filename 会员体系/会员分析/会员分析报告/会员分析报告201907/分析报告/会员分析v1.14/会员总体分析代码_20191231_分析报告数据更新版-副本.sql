--会员总体分析
--代码贡献者：姚泊彰
--代码更新时间：20191231
--数据口径：见各自模块


--简介：会员总体分析总共分为3块：1、会员大数分析；2、会员分析；3、门店品类分析

--0、数据准备
	--0.1、订单范围：20140101-20191231 ；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）
		/*--适用分析 1.1
		with t1 as (
		SELECT
			 "UUID",	   								--明细唯一编码
			 "STSC_DATE",  								--销售日期
			 "SALE_ORDR_DOC",  							--销售订单号
			 "PHMC_CODE",     							--门店编码
			 "GOODS_CODE",    							--商品编码
			 "ORDR_SALE_TIME",  						--订单销售时间
			 "MEMBER_ID",								--会员编码
			 case when member_id is not null then member_id else sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
			 case when member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
			 "ORDR_SOUR_CODE",							--订单来源编码
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
			 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191231'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20140101')) 
		GROUP BY "UUID",								   --明细唯一编码											   
			 "STSC_DATE",                                  --销售日期
			 "SALE_ORDR_DOC",                              --销售订单号
			 "PHMC_CODE",                                  --门店编码
			 "GOODS_CODE",                                 --商品编码
			 "ORDR_SOUR_CODE",							   --订单来源编码
			 "ORDR_SALE_TIME",                             --订单销售时间
			 "MEMBER_ID")                                  --会员编码
		,
		*/
	--0.2、订单范围(20181231-20191231，即一年)；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）,数据源时间在不同分析可能需要修改
		/*--适用分析 2.1.1 , 2.1.3 , 2.2 , 2.7 , 3.1 , 3.3 
		with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 case when weekday("STSC_DATE")+1>=1 and weekday("STSC_DATE")+1<=5 then 'Y' ELSE 'N'  END as is_weekday,
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
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20200101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20190101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
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
				 g.GOODS_NAME
		)
	, 
	--关联得到会员数据	
		t1_1 as (
		select 
			s.stsc_date,
			s.is_weekday,
			s.AT_YEAR,
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,
			s.phmc_code,
			s.sale_amt,
			s.GROS_PROF_AMT,
			s.PURC_MONEY, 	--营销销售
			s.NO_PURC_MONEY,	--非营销
			s.GOODS_CODE,
			s.GOODS_NAME,
			t.phmc_type,
			t.ADMS_ORG_CODE,
			t.ADMS_ORG_NAME,
			c.birthday,
			c.sex,
			C.create_time,
			c.come_from,
			n.DICT_NAME,
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
		from t1 s
		inner join 
		(
			select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				phmc_type
			from dw.dim_phmc
		) t
		on s.phmc_code=t.phmc_code
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
		left join 
		(
			select DICT_NAME,dict_code
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0'
		) n
		on t.phmc_type=n.dict_code
		where is_member='Y' and 				--分析会员占比时关闭 
		not exists
		 (
			  select 1 from dw.DIM_PHMC g1
			  where g1.PHMC_CODE = s.PHMC_CODE 
			  and 
			  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
				   g1.STAR_BUSI_TIME > '20191231' 
				   or 
				   (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
				   or CLOSE_DATE is not null
				   or PROP_ATTR in ('Z02','Z07','Z03','Z04')
			   )
		 )
	),
		--同人同天同门店算一次
		t1_2 as (
		select
			stsc_date,
			member_id,
			phmc_code,
			max(dict_name) as dict_name,
			max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
			max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
			MAX(PHMC_TYPE) As PHMC_TYPE,				--店型
			max(birthday) as birthday,
			max(sex) as sex,
			max(create_time) as create_time,
			sum(sale_amt) as sale_amt,          --销售额
			sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
			sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
			sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
			count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
			1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
		from  t1_1
		group by 
		stsc_date,
		member_id,
		phmc_code
		),
		--同人同天同商品算一次
		t1_3 as (
			SELECT STSC_DATE,
				GOODS_CODE,
				MEMBER_ID,
				SUM(SALE_AMT) AS SALE_AMT,	--销售额
				SUM(GROS_PROF_AMT) AS GROS_PROF_AMT		--毛利额
			FROM t1
			GROUP BY STSC_DATE,
				GOODS_CODE,
				MEMBER_ID
		)
		,
		--得到处理后的品类
		t1_4 as (
		--得到拼接后的品类
		select member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,max(PROD_CATE_LEV1_NAME)  as PROD_CATE_LEV1_NAME
			,sum(SALE_AMT) as SALE_AMT
			,count(1) as sale_times
			,max(birthday) as birthday
		from
		(
			select	stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
				PROD_CATE_LEV2_NAME,
				max(PROD_CATE_LEV1_NAME) as PROD_CATE_LEV1_NAME,	--一级品类
				sum(SALE_AMT) as SALE_AMT,
				max(birthday) as birthday
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
					,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME
					  ,birthday
				,SALE_AMT
				from t1_1 s
				left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
				on S.GOODS_CODE = T.GOODS_CODE
			)
			group by stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
				PROD_CATE_LEV2_NAME
		)
		group by member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
	
	),
	*/
	--0.3、订单范围(20160101-20191231，即三年趋势)；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）,数据源时间在不同分析可能需要修改
		/*--适用分析 2.1.2 , 2.4 , 2.5 , 2.6(需要多取一年) , 3.2 , 3.4
		with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 case when weekday("STSC_DATE")+1>=1 and weekday("STSC_DATE")+1<=5 then 'Y' ELSE 'N'  END as is_weekday,
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
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20200101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20160101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
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
				 g.GOODS_NAME
		)
	,                                
		--得到会员各类信息
		t1_1 as (
		select 
			s.stsc_date,
			s.is_weekday,
			s.AT_YEAR,
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,
			s.phmc_code,
			s.sale_amt,
			s.GROS_PROF_AMT,
			s.PURC_MONEY, 	--营销销售
			s.NO_PURC_MONEY,	--非营销
			s.GOODS_CODE,
			s.GOODS_NAME,
			t.phmc_type,
			t.ADMS_ORG_CODE,
			t.ADMS_ORG_NAME,
			c.birthday,
			c.sex,
			C.create_time,
			case when to_char(create_time,'YYYYMM')=to_char(stsc_date,'YYYYMM') THEN 'new' ELSE 'old' END AS MEMB_TYPE,	--新老会员
			case when days_between(t.STAR_BUSI_TIME,s.stsc_date)<=180 then 'NEW' ELSE 'OD' end as store_type,	--消费是否在开业6个月内
			to_char(create_time,'YYYY') AS CREA_YEAR,		--开卡年份
			c.come_from,
			n.DICT_NAME,
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
		from t1 s
		inner join 
			(
				select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				STAR_BUSI_TIME,
				phmc_type
				from dw.dim_phmc
			) t
		on s.phmc_code=t.phmc_code
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
		left join 
		(
			select DICT_NAME,dict_code
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0'
		) n
		on t.phmc_type=n.dict_code
		where is_member='Y'     
		and not exists
		(
			  select 1 from dw.DIM_PHMC g1
			  where g1.PHMC_CODE = s.PHMC_CODE 
			  and 
			  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
			   g1.STAR_BUSI_TIME > '20191231' 
			   )
		)
	),
	--同人同天同门店算一次
	t1_2 as (
	select
		stsc_date,
		member_id,
		phmc_code,
		to_char(stsc_date,'YYYY') as at_year,
		max(dict_name) as dict_name,
		max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
		max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
		MAX(PHMC_TYPE) As PHMC_TYPE,
		max(birthday) as birthday,
		max(sex) as sex,
		max(CREA_YEAR) as CREA_YEAR,
		max(create_time) as create_time,
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
		sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
		sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
		count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
		1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
	from  t1_1
	group by 
	stsc_date,
	member_id,
	phmc_code
	),
	--使用最终会员ID进行频次合并
	t1_3 as (
		select stsc_date,
			member_id_final as member_id,
			phmc_code,
			to_char(stsc_date,'YYYY') as at_year,
			max(is_member) as is_member,--是否会员
			sum(sale_amt) as sale_amt,          --销售额
			sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
			sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
			sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
			count (distinct GOODS_CODE) as goods_qty --商品类型数量
		from  t1
		group by 
		stsc_date,
		member_id_final,
		phmc_code
	)
	,
	--得到处理后的品类
	t1_4 as (
	select member_id
		,AT_YEAR
		,PROD_CATE_LEV2_NAME
		,max(PROD_CATE_LEV1_NAME)  as PROD_CATE_LEV1_NAME
		,sum(SALE_AMT) as SALE_AMT
		,count(1) as sale_times
		,max(birthday) as birthday
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT
	from
	(
		select	stsc_date,					--日期
			member_id,					--会员ID
			phmc_code,					--门店号
			AT_YEAR,					--年份带出来
			PROD_CATE_LEV2_NAME,
			max(PROD_CATE_LEV1_NAME) as PROD_CATE_LEV1_NAME,	--一级品类
			sum(SALE_AMT) as SALE_AMT,
			max(birthday) as birthday,
			sum(GROS_PROF_AMT) as GROS_PROF_AMT
		from
		(
			select stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
			CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
				  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
				  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
				,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
				  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
				  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME
				  ,birthday
			,SALE_AMT
			,GROS_PROF_AMT
			from t1_1 s
			left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
			on S.GOODS_CODE = T.GOODS_CODE
		)
		group by stsc_date,					--日期
			member_id,					--会员ID
			phmc_code,					--门店号
			AT_YEAR,					--年份带出来
			PROD_CATE_LEV2_NAME
	)
	group by member_id
		,AT_YEAR
		,PROD_CATE_LEV2_NAME
	
	),
	*/
	
		

/*--1、会员现状（大数分析）*/
	/*--1.1、得到会员总数、无消费会员数、线上消费会员数、线下消费会员数、全渠道消费会员数、近一年人均销售额、近一年人均消费次数(需要修改时间)
		--拿到会员主数据，作为所有会员主表，时间截止到20191231
			t2_0 as (
			select memb_code from "DW"."FACT_MEMBER_BASE"
			where CREA_TIME<'20200101'
		)
		--select count(1) from t2_0
		,
		--得到近一年人均销售额、消费次数，时间为20181231-20191231
		t2_1 as (
		select 
		   member_id,
		   sum(sale_amt) as one_year_sale_amt,
		   sum(sale_times) as one_year_sale_times
		from (
			select
				   stsc_date,
				   member_id,
				   phmc_code,
				   sum(sale_amt) as sale_amt, 
				   1 as sale_times 
			from t1
			where stsc_date>=add_years(to_date('20200101','yyyymmdd'),-1)
				  and stsc_date<'20200101'
			group by 
			stsc_date,
			member_id,
			phmc_code
		)
		group by member_id
		)
		,
		--取线上
		t2 as (				
			select 
			    member_id_final as member_id,
			   'online' as source
			from t1
			where is_member='Y'
			      AND ORDR_SOUR_CODE<>'0100'
			group by member_id_final,ORDR_SOUR_CODE
			 ),
			 
			--取线下
		t3 as (	 
			select 
			   member_id_final as member_id,
			  'offline' as source
			from t1
			where is_member='Y'
			      AND (ORDR_SOUR_CODE='0100' OR ORDR_SOUR_CODE IS NULL)
			group by member_id_final
			 ),
			 
			--得到全渠道
		t4 as (
			select 
			    t2.member_id,
			   'on-off-line' as source
			from t2 
			inner join t3
			on t2.member_id=t3.member_id
			group by t2.member_id
		),

         --合并纯线上、纯线下（过滤与全渠道交叉部分）、全渠道
		t5 as ( 
			select 
			     member_id,
			     source
			 from t2
			 where member_id not in (select member_id from t4)
			 union
			 select 
			     member_id,
			     source
			 from t3
			 where member_id not in (select member_id from t4)
			 union
			 select 
			    member_id,
			    source
			 from t4
		)
		,
		--关联得到会员数据
		t6 as (
			select 
				 t1.memb_code as member_id,	--会员ID
				 t2.source,					--消费渠道
				 t3.one_year_sale_amt,		--近一年总销售额
				 t3.one_year_sale_times	--近一年总消费次数
			from t2_0 t1
			left join t5 t2
			on t1.memb_code=t2.member_id
			left join t2_1 t3
			on t1.memb_code=t3.member_id
		)
		,
		--得到会员总数、有消费会员数、未消费会员数
		t7 as(
			select count(1) as memb_num		--总会员数
				,count(source) as memb_consume_num	--有消费会员数
				,count(1)-count(source) as memb_nconsume_num	--未消费会员数
			from t6
		)
		,
		--得到按渠道消费会员消费数据
		t8 as (
			select source,
			 count(1),
			 sum(one_year_sale_amt),		--近一年总销售额
			 sum(one_year_sale_times),		--近一年总消费次数
			 avg(one_year_sale_amt),		--近一年人均销售额
			 avg(one_year_sale_times)		--近一年人均消费次数
			from t6
			group by source
		)
		select * from t7
		
	*/
				
    /*--1.1.1、下钻分析各线上渠道近一年人均销售额、近一年人均消费次数
           ----线上明细查询（需从一年实际销售中查询）
		t6 as (
			select 
				 ORDR_SOUR_CODE,
				 member_id,
				 sum(sale_amt) as one_year_sale_amt,
				 sum(sale_times) as one_year_sale_times
		    from (select
				stsc_date,
				member_id,
				phmc_code,
				max(ORDR_SOUR_CODE) as ORDR_SOUR_CODE,
				sum(sale_amt) as sale_amt, 
				1 as sale_times 
			from t1
			where ORDR_SOUR_CODE<>'0100'
				and stsc_date>=add_years(to_date('20190601','yyyymmdd'),-1)
				and stsc_date<'20190601'
			group by 
				stsc_date,
				member_id,
				phmc_code)
			group by member_id,ORDR_SOUR_CODE
			)


		select 
				ORDR_SOUR_CODE,
				sum(one_year_sale_amt),     --近一年总销售额
				sum(one_year_sale_times),	--近一年总消费次数
				count(*) as qty,            --人数
				avg(one_year_sale_amt),  	--近一年人均销售额
				avg(one_year_sale_times)	--近一年人均消费次数
		from t6 a
		where member_id  in (select member_id from t5 where source='online' )
		group by ORDR_SOUR_CODE
				 ;
    */

--2、会员分析
    --2.1、会员画像
		/*--2.1.1 年龄分段画像数据（近一年）
		--计算各年龄段整体消费情况
		t2 as (
			select 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end as  age,                                            --"年龄"
			count(distinct member_id)  as memb_qty                           --"会员数"
			,sum(sale_amt)/count(distinct member_id) as memb_avg_sale_amt        --"人均消费"
			,sum(sale_times) as sale_times
			,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
			,sum(sale_amt) as sale_amt         --总销售额
			,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
			,sum(goods_qty)  as goods_qty   --SKU总数
			,SUM(PURC_MONEY)/(SUM(PURC_MONEY)+SUM(NO_PURC_MONEY)) AS PURC_RATE		--营销权重占比
			from  t1_2 c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
			and sex in ('男','女')
			group by 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end 
		 ),
			 
			 
		---计算年龄段偏好
		t3 as (
			select age,
				sale_hour,
				row_number() over(partition by age order by sale_times desc) as rn_1
			from 
			(
				select case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end as  age,   
					sale_hour,                                         --"年龄"
					sum(sale_times) as sale_times
				from  
				(
					select stsc_date,
						member_id,
						phmc_code,
						sale_hour,
						max(birthday) as birthday,
						MAX(SEX) AS SEX,
						1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
					from  t1_1
					group by stsc_date,
					member_id,
					phmc_code,
					sale_hour
				) c
				where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				and sex in ('男','女')
				group by 
				 sale_hour,
				case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end
			 )
		 ),
			 
		 --品类专用
		t4_1 as (
			--得到拼接后的品类
			select member_id
				  ,birthday
				,AT_YEAR
				,PROD_CATE_LEV2_NAME
				,sum(SALE_AMT) as SALE_AMT
				,count(1) as sale_times
			from
			(
				select	stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
					PROD_CATE_LEV2_NAME,
					 birthday,
					sum(SALE_AMT) as SALE_AMT
				from
				(
					select stsc_date,					--日期
						member_id,					--会员ID
						phmc_code,					--门店号
						AT_YEAR,					--年份带出来
						birthday,
					CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
						  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
						  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
					,SALE_AMT
					from t1_1 s
					left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
					on S.GOODS_CODE = T.GOODS_CODE
				)
				group by stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
					birthday,
					PROD_CATE_LEV2_NAME
			)
			group by member_id
				,AT_YEAR
				,PROD_CATE_LEV2_NAME
				,birthday
		) ,  

		--计算各品类总次数
		t4_2 as (
			select
			age,
			PROD_CATE_LEV2_NAME,
			sale_times,
			row_number() over(partition by age order by sale_times desc) as rn_1
			from
			(
				select case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end as  age,                                          --"年龄"
					PROD_CATE_LEV2_NAME
					,sum(sale_times) as sale_times
				from  t4_1 c
				where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				group by 
				case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end,                                             --"年龄"
				PROD_CATE_LEV2_NAME
			 ) 
		),

		--取各年龄性别偏好前五的品类，并拼接
		t4 as 
		(
			select age,
			 STRING_AGG(PROD_CATE_LEV2_NAME,'/') as FIVE_PROD_CATE_NAME
			from t4_2 where rn_1<=5
			group by age
		),
			 
		 --计算各年龄段人均门店情况
		t5 as (
			select 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end as  age,                                            --"年龄"
			count(distinct member_id)  as memb_qty                           --"会员数"
			,count(phmc_code)  as phmc_code_qty   --门店数
			from 
			( 
			select 
			member_id,
			phmc_code,
			max(birthday) as birthday
			from t1_1
			group by 
			member_id,
			phmc_code) c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
			group by 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end
		),
			 
			 
			 
		t6 as (
			select GOODS_CODE,
				GOODS_NAME,   
				age,
				sale_times,
				sale_amt,
				GROS_PROF_AMT,
				row_number() over(partition by age order by sale_times desc) as rn_1,
				row_number() over(partition by age order by sale_amt desc) as rn_2,
				row_number() over(partition by age order by GROS_PROF_AMT desc) as rn_3
			from 
			(
				select case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end as  age,   
					GOODS_CODE,
					GOODS_NAME,                                        --"年龄"
					sum(sale_times) as sale_times,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT   --毛利
				from  
				(
					select stsc_date,
						member_id,
						phmc_code,
						GOODS_CODE,
						GOODS_NAME,
						sum(sale_amt) as sale_amt,          --销售额
						sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
						max(birthday) as birthday,
						1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
					from  t1_1
					group by 
					stsc_date,
					member_id,
					phmc_code,
					GOODS_CODE,
					GOODS_NAME
				) c
				where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				group by 
				GOODS_CODE,
				GOODS_NAME,   
				case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
					 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
					 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
					 else  '其他'  end
			 )
		 ),
			 
		 t7 as (
		 select a.age,a.GOODS_NAME as sale_times_GOODS,b.GOODS_NAME as sale_amt_GOODS,c.GOODS_NAME as GROS_GOODS_NAME from t6 a
		 left join t6 b
		 on a.age=b.age
		  left join t6 c
		 on a.age=c.age
		 where  a.rn_1=1 and b.rn_2=1 and c.rn_3=1
		 ),	 
		--汇总t2,t3,t4,t5各计算字段
		t8 as (
		 select t2.*,t3.sale_hour,t4.FIVE_PROD_CATE_NAME,t5.phmc_code_qty,t7.sale_times_GOODS,t7.sale_amt_GOODS,t7.GROS_GOODS_NAME from t2
		 left join t3
		 on  t2.age=t3.age
		 left join t4
		 on  t2.age=t4.age
		 left join t5
		 on  t2.age=t5.age
		 left join t7
		 on  t2.age=t7.age
		 where rn_1=1
		)
		select * from t8
	
	*/
		/*--2.1.2 性别分析（近三年不同性别人均消费额及消费会员数）
		t2 as (
		select 
			AT_YEAR,
			case when sex not in ('男','女') then '未知' 
				when sex  is null then '未知' else sex end  as  sex,              --"性别"
			count(distinct member_id)  as memb_qty                           --"会员数"
			,sum(sale_amt)/count(distinct member_id) as memb_avg_sale_amt        --"人均消费"
			,sum(sale_times) as sale_times
			--,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
			--,sum(sale_amt) as sale_amt         --总销售额
			--,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
		from  t1_2 c
		where  sex in ('男','女')
		group by 
		AT_YEAR,
		case when sex not in ('男','女') then '未知' 
			when sex  is null then '未知' else sex end
		)
		select * from t2
		*/
		/*--2.1.3 年龄性别画像明细数据（近一年）
		t2 as (
			select 
					case when sex not in ('男','女') then '未知' 
						when sex  is null then '未知' else sex end  as  sex,              --"性别"
					case when floor(days_between(c.birthday,now())/365)>=20  
						and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
						 else  '00'  end as  age,                                            --"年龄"
					count(distinct member_id)  as memb_qty                           --"会员数"
					,sum(sale_amt)/count(distinct member_id) as memb_avg_sale_amt        --"人均消费"
					,sum(sale_times) as sale_times
					,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
					,sum(sale_amt) as sale_amt         --总销售额
					,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
					,count(distinct phmc_code)  as phmc_code_qty   --门店数
					,sum(goods_qty)  as goods_qty   --SKU总数
					,SUM(PURC_MONEY)/(SUM(PURC_MONEY)+SUM(NO_PURC_MONEY)) AS PURC_RATE		--营销权重占比
			from  t1_2 c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=80
				  and sex in ('男','女')
			group by 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' 
			   else sex end,
			case when floor(days_between(c.birthday,now())/365)>=20
				and  floor(days_between(c.birthday,now())/365)<=85 then  floor((floor(days_between(c.birthday,now())/365)-20)/5)
				 else  '00'  end
			 ),
			 
			 
			---计算年龄段偏好
			 t3 as (
			select 
					sex,
					age,
					sale_hour,
					sale_times,
					row_number() over(partition by sex,age order by sale_times desc) as rn_1
			from 
			(
			select 
			case when sex not in ('男','女') then '未知' 
				when sex  is null then '未知' else sex end  as  sex,              --"性别"
			case when floor(days_between(c.birthday,now())/365)>=20  
				and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
				 else  '00'  end as  age,  
			sale_hour,                                          --"年龄"
			sum(sale_times) as sale_times
			from  
			(
				select
				stsc_date,
				member_id,
				phmc_code,
				sale_hour,
				max(birthday) as birthday,
				max(sex) as sex,
				1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code,
			sale_hour) c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				  and sex in ('男','女')
			group by 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' 
			   else sex end,
			 sale_hour,
			case when floor(days_between(c.birthday,now())/365)>=20
				and  floor(days_between(c.birthday,now())/365)<=85 then  floor((floor(days_between(c.birthday,now())/365)-20)/5)
				 else  '00'  end
			 )
			 ),
			 
			 --品类专用
				t4_1 as (
					--得到拼接后的品类
					select member_id
						  ,birthday
						 ,sex
						,AT_YEAR
						,PROD_CATE_LEV2_NAME
						,sum(SALE_AMT) as SALE_AMT
						,count(1) as sale_times
					from
					(
						select	stsc_date,					--日期
							member_id,					--会员ID
							phmc_code,					--门店号
							AT_YEAR,					--年份带出来
							PROD_CATE_LEV2_NAME,
							 birthday,
							 sex,
							sum(SALE_AMT) as SALE_AMT
						from
						(
							select stsc_date,					--日期
								member_id,					--会员ID
								phmc_code,					--门店号
								AT_YEAR AS AT_YEAR,					--年份带出来
								birthday,
								sex,
							CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
								  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
								  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
							,SALE_AMT
							from t1_1 s
							left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
							on S.GOODS_CODE = T.GOODS_CODE
						)
						group by stsc_date,					--日期
							member_id,					--会员ID
							phmc_code,					--门店号
							AT_YEAR,					--年份带出来
							birthday,
							sex,
							PROD_CATE_LEV2_NAME
					)
					group by member_id
						,AT_YEAR
						,PROD_CATE_LEV2_NAME
						,birthday
						,sex
				) ,  

			--计算各品类总次数
			t4_2 as 
			(select
					sex,
					age,
					PROD_CATE_LEV2_NAME,
					sale_times,
					row_number() over(partition by sex,age order by sale_times desc) as rn_1
			from
			(
				select 	
			case when sex not in ('男','女') then '未知' 
				when sex  is null then '未知' else sex end  as  sex,              --"性别"
			case when floor(days_between(c.birthday,now())/365)>=20  
				and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
				 else  '00'  end as  age,                                             --"年龄"
			PROD_CATE_LEV2_NAME
			,sum(sale_times) as sale_times
			from  t4_1 c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				  and sex in ('男','女')
			group by 
			case when sex not in ('男','女') then '未知' 
				when sex  is null then '未知' else sex end,              --"性别"
			case when floor(days_between(c.birthday,now())/365)>=20  
				and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
				 else  '00'  end,                                             --"年龄"
			PROD_CATE_LEV2_NAME
			 ) 
			),

			--取各年龄性别偏好前五的品类，并拼接
			t4 as 
			(
			select 
			 sex,
			age,
			 STRING_AGG(PROD_CATE_LEV2_NAME,'/') as FIVE_PROD_CATE_NAME
			 from t4_2 where rn_1<=5
			 group by  sex,
			age
			)
			, 
			 --计算各年龄段人均门店情况
			t5 as (
			select 
					case when sex not in ('男','女') then '未知' 
						when sex  is null then '未知' else sex end  as  sex,              --"性别"
					case when floor(days_between(c.birthday,now())/365)>=20  
						and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
						 else  '00'  end as  age,                                            --"年龄"
					count(distinct member_id)  as memb_qty                           --"会员数"
					,count(phmc_code)  as phmc_code_qty   --门店数
			from 
			( 
			select 
					member_id,
					phmc_code,
					max(birthday) as birthday,
					max(sex) as sex
			from t1_1
			group by 
			member_id,
			phmc_code) c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
				  and sex in ('男','女')
			group by 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' 
			   else sex end,
			case when floor(days_between(c.birthday,now())/365)>=20
				and  floor(days_between(c.birthday,now())/365)<=85 then  floor((floor(days_between(c.birthday,now())/365)-20)/5)
				 else  '00'  end
			 )
			 
		--汇总t2,t3,t4,t5各计算字段
		t6 as( 
			 select t2.*,t3.sale_hour, t3.sale_times,t4.FIVE_PROD_CATE_NAME,t5.phmc_code_qty from t2
			 left join t3
			 on t2.sex=t3.sex
			 and t2.age=t3.age
			 left join t4
			 on t2.sex=t4.sex
			 and t2.age=t4.age
			 left join t5
			 on t2.sex=t5.sex
			 and t2.age=t5.age
			 where rn_1=1;
		 )
		 select * from t6
		*/
		
	/*--2.2、会员消费金额与消费频次(近一年)
	--消费金额分布
	t2 as (
		select 
			case when sale_amt<50 then '50元以下'
				 when sale_amt<100 then '50-100元'
				 when sale_amt<200 then '100-200元'
				 when sale_amt<500 then '200-500元'
				 when sale_amt<1000 then '500-1000元'
				 when sale_amt<2000 then '1000-2000元'
				 else '2000元以上' end as  fl,
				 count(member_id) as memb_qty
			from
			(
				select 
				member_id
				,sum(sale_amt)sale_amt
				from t1_2
				--where  member_id is not null
			   group by member_id	
			) a
			group by 
			case when sale_amt<50 then '50元以下'
				 when sale_amt<100 then '50-100元'
				 when sale_amt<200 then '100-200元'
				 when sale_amt<500 then '200-500元'
				 when sale_amt<1000 then '500-1000元'
				 when sale_amt<2000 then '1000-2000元'
				 else '2000元以上' end
		)
	,
				
	--消费频次分布
	t3 as (
		select 
			case when sale_ordr_doc<4 then sale_ordr_doc||'次'
				 when sale_ordr_doc<8 then '4-7'||'次'
				 when sale_ordr_doc<13 then '8-12次'
				 when  sale_ordr_doc<19 then '13-18次'
				 when   sale_ordr_doc>=19 then '19次以上'
				 else sale_ordr_doc||'次' end   as sale_numb,                     --"消费频次"
				 count(member_id)  as memb_qty                                    --"会员数"
			from 
			(
				select 
				member_id,
				--,count(distinct sale_ordr_doc) sale_ordr_doc
				--from :VAR_0_1
				sum(sale_times) as sale_ordr_doc                      --add by xueyan
				from t1_2
				--where  member_id is not null
			   group by member_id	
			) a
			group by
			case when sale_ordr_doc<4 then sale_ordr_doc||'次'
				 when sale_ordr_doc<8 then '4-7'||'次'
				 when sale_ordr_doc<13 then '8-12次'
				 when sale_ordr_doc<19 then '13-18次'
				 when sale_ordr_doc>=19 then '19次以上'
				 else sale_ordr_doc||'次' end
	)
	,
	--明细金额分布
	t4 as (
	select 
		 round((sale_amt-20)/40)*40+20 as  sale_amt
		,count(member_id) as memb_qty
		from
		(
			select 
			member_id
			,sum(sale_amt) sale_amt
			from t1_2
			--where  member_id is not null
		   group by member_id	
		) a
		group by 
		 round((sale_amt-20)/40)*40+20
	)
	select * from t2
	*/
	
	/*--2.3、会员注册渠道（不需要消费数据）
	--各渠道会员来源
	with t2 as (
		select MEMB_CODE,
		to_char(CREA_TIME,'yyyy') AS CREA_YEAR,
		case when MEMB_SOUR in ('ALIPAY','ZFM') then '支付宝'
			  when MEMB_SOUR in ('WX','WXM','MMP','WXVIP') then '微信'
			  when MEMB_SOUR in ('ST') THEN '门店' 
			  else '其他' end as MEMB_SOUR_FLAG
		,MEMB_SOUR
		 from 
		"DW"."FACT_MEMBER_BASE"
		where 
		CREA_TIME<'20200101'      --时间自行调整
		and CREA_TIME>='20180101'
		and MEMB_SOUR not in ('SG','SG_XX','JM')
	),
	--其他渠道
	t3 as (
		select MEMB_SOUR
			dict_code,dict_name,num
		from 
		(
			SELECT MEMB_SOUR
				,COUNT(1) AS NUM
			FROM T2
			WHERE CREA_YEAR='2019'
			AND MEMB_SOUR_FLAG='其他'
			GROUP BY MEMB_SOUR
		) a
		left join (select dict_code,dict_name from "DS_POS"."SYS_DICT" ) b
		on a.MEMB_SOUR=b.dict_code
	)
	--SELECT CREA_YEAR,MEMB_SOUR_FLAG,COUNT(1) FROM T2 GROUP BY CREA_YEAR,MEMB_SOUR_FLAG
	select * from t3 where dict_name is not null order by num desc
	*/
	/*--2.4、订单分析
	--得到会员非会员各年订单数和客单价(近三年)
	t2 as (
		select at_year				--年份
			,is_member				--是否会员
			,count(1) as order_num	--订单数
			,avg(sale_amt) as unit_pri	--客单价
		from t1_3
		group by at_year
			,is_member
		
	)
	select * from t2
	*/
	
	/*--2.5、年新增会员分析
		--过滤得到年新增会员
		t2 as (
			SELECT AT_YEAR
				,COUNT(MEMBER_ID) AS MEMB_NUM
				,SUM(sale_amt) AS sale_amt
				,AVG(SALE_TIMES) AS AVG_SALE_TIMES
				,AVG(sale_amt) AS AVG_sale_amt
			FROM
			(
				select AT_YEAR
					,MEMBER_ID
					,sum(sale_amt) as sale_amt          --销售额
					,sum(GROS_PROF_AMT) as GROS_PROF_AMT    --毛利
					,COUNT(1) AS SALE_TIMES		--消费次数
				from t1_2
				where at_year=CREA_YEAR
				GROUP BY AT_YEAR
					,MEMBER_ID
			)GROUP BY AT_YEAR
		)
		SELECT * FROM t2
	*/
	
	--2.6、年复购会员分析
		/*年复购逻辑处理(复购逻辑必须放开该代码)
	--每人按年统计
	t2_0 as
	(
		select 
				 AT_YEAR,
				 MEMBER_ID,
				 sum(SALE_AMT) as SALE_AMT,
				 count(1) as sale_times,
				 sum(PURC_MONEY) as PURC_MONEY--营销销售额
			 from t1_2 
			 group by AT_YEAR,
				MEMBER_ID
	)
	,
	--得到会员所属门店是否收购加盟、是否新老店、是哪个分公司
	--首先，得到会员所属门店
	t2_1 as
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
				from t2_0
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
	t2_2 as
	(
		select  t1.AT_YEAR
			,t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.PURC_MONEY	--营销销售额
			,t1.sale_times
			,case when left(t2.company_code,1)=4 or t2.PROP_ATTR in ('Z02','Z07') then 'SG_JM' else 'NORMAL' end as SG_JM_FLAG		--是否收购加盟
			,t2.ADMS_ORG_NAME		--分公司名称
		from t2_0 t1 
		left join t2_1 t2
		on t1.MEMBER_ID=t2.member_id
	)
	,
	--得到每个会员年复购情况
	t2_3 as (
		select  
			 t1.AT_YEAR,				--年份
			 t1.SG_JM_FLAG,
			 t1.ADMS_ORG_NAME,			--分公司名称
			 t2.member_id ,				--上一年是否购买
			 t1.SALE_AMT	,			--销售额
			 t1.sale_times				--消费次数
			 ,t1.PURC_MONEY	--营销销售额
		from t2_2 t1
		left join t2_2 t2
		on t1.AT_YEAR=t2.AT_YEAR+1
		and t1.member_id=t2.member_id
	)
	,
	--得到每个会员每年每个品类消费情况
	t2_4 as (
	select  
			 t1.AT_YEAR,				--年份
			 t1.PROD_CATE_LEV2_NAME,
			 t2.member_id ,				--上一年是否购买
			 t1.SALE_AMT	,			--销售额
			 t1.sale_times				--消费次数
		from t1_4 t1
		left join t1_4 t2
		on t1.AT_YEAR=t2.AT_YEAR+1
		and t1.member_id=t2.member_id
		and t1.PROD_CATE_LEV2_NAME=t2.PROD_CATE_LEV2_NAME
	)
	*/
		/*--2.6.1 总体年复购情况
	t3_1 as (
		select	AT_YEAR				--年份
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--复购营销金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from t2_3
		group by AT_YEAR
	)
	,
	t3_2 as ( 
		select 
			AT_YEAR,
			count(1) as total_qty --消费会员总数	
		from t2_3
		group by AT_YEAR
	 )
	,t3 as (
		select 	 
			t5.AT_YEAR,					--年份
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times, 		--总消费次数
			t4.return_memb_sale_purc	--复购营销金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--人均复购营销金额
		from t3_2 t5
		left join t3_1 t4
		on t5.AT_YEAR=t4.AT_YEAR
	)
	select * from t3
	*/
		/*--2.6.2 收购加盟影响
	t4_1 as (
		select	AT_YEAR				--年份
			,SG_JM_FLAG
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--复购营销金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from t2_3
		group by AT_YEAR
			,SG_JM_FLAG
	)
	,
	t4_2 as ( 
		select 
			AT_YEAR
			,SG_JM_FLAG
			,count(1) as total_qty --消费会员总数	
		from t2_3
		group by AT_YEAR
			,SG_JM_FLAG
	 )
	,t4 as (
		select 	 
			t5.AT_YEAR,					--年份
			t5.total_qty,				--总消费人数
			t5.SG_JM_FLAG,				--是否收购加盟
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times, 		--总消费次数
			t4.return_memb_sale_purc	--复购营销金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--人均复购营销金额
		from t4_2 t5
		left join t4_1 t4
		on t5.AT_YEAR=t4.AT_YEAR
		and t5.SG_JM_FLAG=t4.SG_JM_FLAG
	)
	select * from t4
	*/
		/*--2.6.3 各分公司年复购情况
		t4_1 as (
			select	AT_YEAR				--年份
				,ADMS_ORG_NAME
				,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
				,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
				,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--复购营销金额
				,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
			from t2_3
			group by AT_YEAR
				,ADMS_ORG_NAME
		)
		,
		t4_2 as ( 
			select 
				AT_YEAR
				,ADMS_ORG_NAME
				,count(1) as total_qty --消费会员总数	
			from t2_3
			group by AT_YEAR
				,ADMS_ORG_NAME
		 )
		,t4 as (
			select 	 
				t5.AT_YEAR,					--年份
				t5.total_qty,				--总消费人数
				t5.ADMS_ORG_NAME,				--是否收购加盟
				t4.return_memb_num,			--复购人数	
				t4.return_memb_sale, 		--总销售额
				t4.return_memb_times, 		--总消费次数
				t4.return_memb_sale_purc	--复购营销金额
				,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
				,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
				,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--人均复购营销金额
			from t4_2 t5
			left join t4_1 t4
			on t5.AT_YEAR=t4.AT_YEAR
			and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
		)
		select * from t4
		*/
		/*--2.6.4 各品类年复购情况
		t4_1 as (
			select	AT_YEAR				--年份
				,PROD_CATE_LEV2_NAME
				,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
				,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
				--,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--复购营销金额
				,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
			from t2_4
			group by AT_YEAR
				,PROD_CATE_LEV2_NAME
		)
		,
		t4_2 as ( 
			select 
				AT_YEAR
				,PROD_CATE_LEV2_NAME
				,count(1) as total_qty --消费会员总数	
			from t2_4
			group by AT_YEAR
				,PROD_CATE_LEV2_NAME
		 )
		,t4 as (
			select 	 
				t5.AT_YEAR,					--年份
				t5.total_qty,				--总消费人数
				t5.PROD_CATE_LEV2_NAME,				--是否收购加盟
				t4.return_memb_num,			--复购人数	
				t4.return_memb_sale, 		--总销售额
				t4.return_memb_times 		--总消费次数
				--t4.return_memb_sale_purc	--复购营销金额
				,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
				,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
				--,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--人均复购营销金额
			from t4_2 t5
			left join t4_1 t4
			on t5.AT_YEAR=t4.AT_YEAR
			and t5.PROD_CATE_LEV2_NAME=t4.PROD_CATE_LEV2_NAME
		)
		select * from t4
		
		*/
	
	/*--2.7、疾病分析
	--得到疾病-药数据
	--首先得到主治用药，并得到每个药的条数
	t2_0 as (
		select GOODS_CODE,count(1) as num from "EXT_TMP"."BOZHANG_MEDI_DISEASE" where POINT=2 group by GOODS_CODE
	)
	--step2.1 然后，取出疾病表中只有一条主治用药的单品
	,
	t2_1 as (
		select t0.GOODS_CODE,t1.DISEASE_NAME_LEV1,t1.DISEASE_NAME_LEV2 from
		(
			select GOODS_CODE from t2_0 where num=1
		)t0
		left join 
		(
			SELECT * FROM "EXT_TMP"."BOZHANG_MEDI_DISEASE" where POINT=2
		)
		t1 
		on t0.GOODS_CODE=t1.GOODS_CODE
	)
	,
	--拿step1中的数据与step2.1关联得到每个会员每天买过的疾病情况，并过滤掉疾病为空的天数
	t3 as (
		select MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
		FROM (
			select  t1.MEMBER_ID,						--会员编码
			 t1.STSC_DATE,  					--销售日期
			 t1.GOODS_CODE,    				--商品编码
			 t2.DISEASE_NAME_LEV1,			--疾病一级
			 t2.DISEASE_NAME_LEV2			--疾病二级
			from t1_3 t1
			left join t2_1 t2
			on t1.GOODS_CODE=t2.GOODS_CODE
		)
		WHERE  DISEASE_NAME_LEV2 is not null 
		group by MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
	)
	,
	--step4:对每个会员数据按照疾病进行汇总，（然后把该表打横作为标签，在视图标签中实现，可以结合来看实现方式）
	t4 as (
		select member_id,DISEASE_NAME_LEV2,count(1) as day_num 
		from t3
		group by member_id,DISEASE_NAME_LEV2
	)
	--step5:统计每个疾病中会员的相关数据
	,
	t5 as(
		SELECT t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.GROS_PROF_AMT
			,t1.NUM as sale_times
			,t4.DISEASE_NAME_LEV2
		FROM
		(
			select member_id
				,SUM(SALE_AMT) AS SALE_AMT
				,SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
				,COUNT(1) AS NUM
			from
			(
				select STSC_DATE,
					PHMC_CODE,
					MEMBER_ID,
					SALE_AMT,	--销售额
					GROS_PROF_AMT --毛利额
				FROM t1_2 
			)
			group by member_id
		) t1 
		LEFT JOIN t4 
		on t1.MEMBER_ID=t4.member_id
	)
	,
	t6 as (
		select DISEASE_NAME_LEV2
		,count(distinct member_id) as memb_num --会员数
		,avg(SALE_AMT) as memb_year_sale
		,avg(GROS_PROF_AMT) as memb_year_gros
		,avg(sale_times) as memb_year_times
		from t5
		group by DISEASE_NAME_LEV2
	)
	select * from t6
	*/
	
--3、门店品类分析
	/*--3.1、品类销售结构（一年）
	--总体结构
	t2 as (
		select 	
		PROD_CATE_LEV1_NAME,
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_times) as sale_times									--消费次数
		,sum(sale_times) /count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		--,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
		from t1_4
		group by 
			PROD_CATE_LEV1_NAME
		order by 
			PROD_CATE_LEV1_NAME
	)
	,
	--各年龄段品类销售结构
	t3 as (
	select 	case when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
		 when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
		 when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
		 when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
		 when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
		 when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
		 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
		 when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
		 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
		 when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
		 when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
		 when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
		 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
		 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
		 else  '85以上'  end as  age, --"年龄"
		PROD_CATE_LEV1_NAME,
		sum(sale_amt) as sale_amt         --总销售额
	from  t1_4 c
	group by 
	PROD_CATE_LEV1_NAME,
		case when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
		 when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
		 when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
		 when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
		 when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
		 when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
		 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
		 when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
		 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
		 when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
		 when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
		 when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
		 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
		 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
		 else  '85以上'  end
	)
	,t4 as (
		select case when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
			when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
			when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
			when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
			when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
			when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
			when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
			when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
			when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
			when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
			when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
			when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
			when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
			when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
			else  '85以上'  end as  age, --"年龄"
			sum(sale_amt) as total_sale_amt         --总销售额
		from  t1_4 c
		group by 
		case when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
			when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
			when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
			when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
			when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
			when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
			when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
			when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
			when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
			when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
			when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
			when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
			when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
			when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
			else  '85以上'  end
	)
	,
	 --计算每个品类在年龄段中的销售占比
	t5 as (
	 select 
		 t3.age,
		 t3.PROD_CATE_LEV1_NAME,
		 t3.sale_amt/t4.total_sale_amt         --占比
	 from t3 
	 left join t4 
	 on t3.age=t4.age
	 )
	select * from t2
	--select * from t5 order by age,PROD_CATE_LEV1_NAME
	*/
	/*--3.2、品类趋势(三年)
	t2_1 as (
		select	AT_YEAR				--年份
			,PROD_CATE_LEV2_NAME
			,sum(sale_amt) as sale_amt   --销售额
			,sum(GROS_PROF_AMT)/sum(sale_amt) as gros_rate	--毛利率
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM		--购买会员数
			,sum(sale_times)/count(distinct member_id) as memb_avg_ordr	--人均消费频次
		from t1_4
		group by AT_YEAR
			,PROD_CATE_LEV2_NAME
	)
	,
	t2_2 as ( 
		select 
			AT_YEAR
			,sum(sale_amt) as sale_amt   --销售额
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM		--购买会员数
		from t1_4
		group by AT_YEAR
	 )
	,t2 as (
		select 	 
			t1.AT_YEAR,					--年份
			t1.PROD_CATE_LEV2_NAME,				
			t1.sale_amt/t2.sale_amt as sale_rate,	--销售占比			
			t1.gros_rate,				--毛利率
			t1.MEMB_NUM/t2.MEMB_NUM as memb_rate, 		--会员渗透率
			t1.memb_avg_ordr 		--人均消费频次
		from t2_1 t1
		left join t2_2 t2
		on t1.AT_YEAR=t2.AT_YEAR
	)
	select * from t2
	*/
	/*--3.3、各店型门店会员销售占比分析（一年，注，使用源时需要取消源头必须是会员的限制）
	t2 as (
		select PHMC_CODE
			   ,PHMC_TYPE
				,sum(SALE_AMOUNT)/count(1) as SALE_AMOUNT_month
				,sum(SALE_MEMB_AMOUNT)/count(1) as SALE_MEMB_AMOUNT_month
				,sum(SALE_MEMB_AMOUNT/SALE_AMOUNT)/count(1) as SALE_RATE
		from 
		(select PHMC_CODE
				,PHMC_TYPE
				,to_char(stsc_date,'YYYYMM') AS stsc_MONTH
				,SUM(SALE_AMT) AS SALE_AMOUNT
				,SUM(CASE WHEN MEMBER_ID IS NOT NULL THEN SALE_AMT ELSE 0 END) AS SALE_MEMB_AMOUNT
			from t1_2
			group by PHMC_CODE,PHMC_TYPE,to_char(stsc_date,'YYYYMM')
			having SUM(SALE_AMT)<>0
		) a
		group by PHMC_CODE,PHMC_TYPE
	)
	,

--按照分段进行门店数统计及销售占比最大最小值
	t3 as(
		select PHMC_TYPE
			,dict_name
			,count(1) as num  --数量
			,max(SALE_RATE) as max_sale_rate --最大会员销售占比
			,min(SALE_RATE) as min_sale_rate --最小会员销售占比
			,max(percent_50) as sale_rate_percent_50 --会员销售占比中位值
			,sum(SALE_MEMB_AMOUNT_month)/sum(SALE_AMOUNT_month) as  avg_sale_rate--会员销售占比平均值
		from
		(
			select PHMC_CODE
				,PHMC_TYPE
				--,dict_name
				,SALE_RATE
				,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY SALE_RATE ASC) over(partition by PHMC_TYPE) as percent_50
				,SALE_AMOUNT_month
				,SALE_MEMB_AMOUNT_month
			from
			(
				select PHMC_CODE
					,PHMC_TYPE
					--,dict_name
					--,case when floor(SALE_AMOUNT_month/100000)>10 then 10 else floor(SALE_AMOUNT_month/100000) end as sale_level
					,SALE_RATE
					,SALE_AMOUNT_month
					,SALE_MEMB_AMOUNT_month
				from t2
			)
		) a
		left join 
		(
			select * 
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0'
		) n
		on a.phmc_type=n.dict_code
		group by PHMC_TYPE,dict_name
	)
	
	select * from t3
	
	*/
	/*--3.4、各店型四项值分析（消费会员数、平均消费金额、新增会员数、复购会员数）(拿三年看)
	--计算每月购买人数与金额
	t2 as (
		select   PHMC_CODE
			,PHMC_TYPE
			,store_type
			,sum(memb_num)
			,sum(SALE_MEMB_AMOUNT)
			,sum(memb_num)/count(1) as memb_num_avg_month						--会员每月购买人数
			,sum(SALE_MEMB_AMOUNT/memb_num)/count(1) as memb_sale_avg_month		--平均每个会员每月购买金额
		from
		(select PHMC_CODE
				,PHMC_TYPE
				,store_type
				,to_char(stsc_date,'YYYYMM') AS stsc_MONTH
				,SUM(SALE_AMT) AS SALE_MEMB_AMOUNT		--总销售金额
				,count(distinct member_id) as memb_num	--会员数量
				from t1_1  
				where member_id is not null                         --add by xueyan 
		group by PHMC_TYPE,PHMC_CODE,store_type,to_char(stsc_date,'YYYYMM')
		) a
		group by PHMC_TYPE,PHMC_CODE,store_type
	)
	,
	--得到每个月购买会员的新老会员情况
	t3 as(
		select   PHMC_CODE
			,PHMC_TYPE
			,store_type
			,sum(MEMB_NEW_NUM)/count(1) as memb_new_num_avg_month						--新会员每月购买人数
			,sum(MEMB_OLD_NUM)/count(1) as memb_old_num_avg_month						--老会员每月购买人数
		from
		(select PHMC_CODE
				,PHMC_TYPE
				,store_type
				,to_char(stsc_date,'YYYYMM') AS stsc_MONTH
				,sum(CASE WHEN MEMB_TYPE='new' then 1 else 0 end) AS MEMB_NEW_NUM		--月新增消费会员数
				,sum(CASE WHEN MEMB_TYPE='old' then 1 else 0 end) AS MEMB_OLD_NUM		--月老消费会员数
				from t1_1  
				where member_id is not null                         --add by xueyan 
		group by PHMC_TYPE,PHMC_CODE,store_type,to_char(stsc_date,'YYYYMM')
		) a
		group by PHMC_TYPE,PHMC_CODE,store_type
	)
	,
	t4 as (
		--计算中位数
			select 
				PHMC_TYPE,
				dict_name,
				store_type,
				max(memb_num_percent_50) as memb_num_percent_50,
				max(memb_sale_percent_50) as memb_sale_percent_50
				from 
			(
				select 
				PHMC_TYPE,
				phmc_code,
				store_type,	 
				memb_num_avg_month,
				memb_sale_avg_month,
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_num_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_num_percent_50,
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_sale_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_sale_percent_50
				from t2
			) a
			left join 
			(
				select * 
				from "DS_POS"."SYS_DICT"
				 where type_code='m_shopType' and deletelable='0'
			) n
			on a.phmc_type=n.dict_code
			group by PHMC_TYPE,dict_name,store_type
	
	)
	,
	t5 as(
		select 
				PHMC_TYPE,
				dict_name,
				store_type,
				max(memb_new_percent_50) as memb_new_percent_50,
				max(memb_old_percent_50) as memb_old_percent_50
				from 
			(
				select 
				PHMC_TYPE,
				phmc_code,
				store_type,	
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_new_num_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_new_percent_50,
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_old_num_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_old_percent_50
				from t3
			) a
			left join 
			(
				select * 
				from "DS_POS"."SYS_DICT"
				 where type_code='m_shopType' and deletelable='0'
			) n
			on a.phmc_type=n.dict_code
			group by PHMC_TYPE,dict_name,store_type
	)
	,
	--合并所有结果
	t6 as (
		select t4.PHMC_TYPE,
			t4.dict_name,
			t4.store_type,
			t4.memb_num_percent_50,
			t4.memb_sale_percent_50,
			t5.memb_new_percent_50,
			t5.memb_old_percent_50
		from t4
		left join t5
		on t4.PHMC_TYPE=t5.PHMC_TYPE
		and t4.dict_name=t5.dict_name
		and t4.store_type=t5.store_type
		
	)
	select * from t6
	
	*/
	
	
	
	
	
	
	
	
	
	
	