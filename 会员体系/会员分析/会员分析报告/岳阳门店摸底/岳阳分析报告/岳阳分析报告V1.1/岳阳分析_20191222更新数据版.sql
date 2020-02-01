--岳阳会员分析
--代码贡献者：薛艳
--代码更新时间：20190822
--数据口径：见各自模块

--简介：会员总体分析总共分为3块：1、门店分析；2、大数；3、会员到年；4、会员画像；5、会员权益；6、品类；

--0、数据准备
	--0.1、订单数据：岳阳所有订单(20140101至今)；订单数据基础过滤
	/*--适用范围：1.1
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
			 t1.PHMC_TYPE AS PHMC_CATE,				--是否自营
			 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
			 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
			 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
			 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
			 ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191223'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20140101')) t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		inner join "EXT_TMP"."YUEYANG_STORE" t1 on t.PHMC_CODE=t1.PHMC_CODE 
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
	--得到岳阳会员总池子
	t1_1 as (
		select 
			customer_id as member_id,
			d.phmc_type as OPEN_PHMC_CATE  
		from ds_crm.tp_cu_customerbase c
		inner join "EXT_TMP"."YUEYANG_STORE" d
		on c.store=d.phmc_code
		WHERE C.CREATE_TIME<='20191222'
	)
	,
		
	*/
	--0.2、订单数据：岳阳所有订单(20180101至今)；订单数据基础过滤
	/*--适用范围：2.1 、 3.3
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
				 t1.PHMC_TYPE AS PHMC_CATE,				--是否自营
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
                 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191223'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20180101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			inner join "EXT_TMP"."YUEYANG_STORE" t1 on t.PHMC_CODE=t1.PHMC_CODE 
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 t1.PHMC_TYPE,
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME
		)
     --关联得到会员信息
	 ,t1_1 as (
			select 
			s.stsc_date,					--销售日期
			s.is_weekday,					--是否工作日
			s.AT_YEAR,						--销售年份
			S.AT_MONTH,						--销售年月
			s.member_id_final,				--最终会员号
			s.IS_MEMBER,					--是否会员
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,					--会员号
			s.PHMC_CATE,				--是否自营
			T.STAR_BUSI_MONTH,			--开业年月
			s.phmc_code,				--销售门店
			s.sale_amt,					--销售金额
			s.GROS_PROF_AMT,			--销售毛利
			s.GOODS_CODE,				--商品编码
			s.GOODS_NAME,				--商品名
			c.OPEN_PHMC_CATE,			--直营加盟开卡会员
			t.phmc_type,				--门店类型
			c.birthday,					--生日
			c.sex,						--性别
			C.create_time,				--会员创建时间
			c.create_year,				--会员创建年份
			case when c.create_year=s.at_year then 1 else 0 end as memb_type,		--会员类型（年新增和复购）
			c.come_from,				--会员渠道
			n.phmc_type_name,			--门店类型名称
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
			from t1 s
			inner join 
			(
				select 
				a.phmc_code,
				a.phmc_type,
				to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_MONTH
				from dw.dim_phmc a
			) t
			on s.phmc_code=t.phmc_code
			left join  
			( 
				select 
				customer_id,
				birthday,
				sex,
				create_time,
				to_char(create_time,'yyyy') as create_year,
				come_from,
				d.phmc_type as OPEN_PHMC_CATE  
				from ds_crm.tp_cu_customerbase c
				left join "EXT_TMP"."YUEYANG_STORE" d
				on c.store=d.phmc_code
			) c 
			on s.member_id=c.customer_id
			left join 
			(
				select DICT_NAME as phmc_type_name,dict_code
				from "DS_POS"."SYS_DICT"
				 where type_code='m_shopType' and deletelable='0'
			) n
			on t.phmc_type=n.dict_code
		)
	,
	--同人同天同门店算一次
	t1_2 as (
	select
		stsc_date,
		member_id,
		phmc_code,
		to_char(stsc_date,'YYYY') as at_year,
		--max(dict_name) as dict_name,
		--max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
		--max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
		MAX(PHMC_TYPE) As PHMC_TYPE,
		max(PHMC_CATE) as PHMC_CATE,
		max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,
		max(birthday) as birthday,
		max(sex) as sex,
		max(CREATE_YEAR) as CREA_YEAR,
		max(create_time) as create_time,
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
		--sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
		--sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
		count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
		1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
	from  t1_1
	group by 
	stsc_date,
	member_id,
	phmc_code
	)
    ,
	*/
	--0.3、订单数据：岳阳所有订单(20160101至今)；订单数据基础过滤
	/*--适用范围：3.1 、 3.2 、 4.2
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
				 t1.PHMC_TYPE AS PHMC_CATE,				--是否自营
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
                 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191223'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20160101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			inner join "EXT_TMP"."YUEYANG_STORE" t1 on t.PHMC_CODE=t1.PHMC_CODE 
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 t1.PHMC_TYPE,
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME
		)
     --关联得到会员信息
	 ,t1_1 as (
			select 
			s.stsc_date,					--销售日期
			s.is_weekday,					--是否工作日
			s.AT_YEAR,						--销售年份
			S.AT_MONTH,						--销售年月
			s.member_id_final,				--最终会员号
			s.IS_MEMBER,					--是否会员
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,					--会员号
			s.PHMC_CATE,				--是否自营
			T.STAR_BUSI_MONTH,			--开业年月
			s.phmc_code,				--销售门店
			s.sale_amt,					--销售金额
			s.GROS_PROF_AMT,			--销售毛利
			s.GOODS_CODE,				--商品编码
			s.GOODS_NAME,				--商品名
			c.OPEN_PHMC_CATE,			--直营加盟开卡会员
			t.phmc_type,				--门店类型
			c.birthday,					--生日
			c.sex,						--性别
			C.create_time,				--会员创建时间
			c.create_year,				--会员创建年份
			case when c.create_year=s.at_year then 1 else 0 end as memb_type,		--会员类型（年新增和复购）
			c.come_from,				--会员渠道
			n.phmc_type_name,			--门店类型名称
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
			from t1 s
			inner join 
			(
				select 
				a.phmc_code,
				a.phmc_type,
				to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_MONTH
				from dw.dim_phmc a
			) t
			on s.phmc_code=t.phmc_code
			left join  
			( 
				select 
				customer_id,
				birthday,
				sex,
				create_time,
				to_char(create_time,'yyyy') as create_year,
				come_from,
				d.phmc_type as OPEN_PHMC_CATE  
				from ds_crm.tp_cu_customerbase c
				left join "EXT_TMP"."YUEYANG_STORE" d
				on c.store=d.phmc_code
			) c 
			on s.member_id=c.customer_id
			left join 
			(
				select DICT_NAME as phmc_type_name,dict_code
				from "DS_POS"."SYS_DICT"
				 where type_code='m_shopType' and deletelable='0'
			) n
			on t.phmc_type=n.dict_code
		)
	,
	--同人同天同门店算一次
	t1_2 as (
	select
		stsc_date,
		member_id,
		phmc_code,
		to_char(stsc_date,'YYYY') as at_year,
		--max(dict_name) as dict_name,
		--max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
		--max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
		MAX(PHMC_TYPE) As PHMC_TYPE,
		max(PHMC_CATE) as PHMC_CATE,
		max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,
		max(birthday) as birthday,
		max(sex) as sex,
		max(CREATE_YEAR) as CREA_YEAR,
		max(create_time) as create_time,
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
		--sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
		--sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
		count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
		1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
	from  t1_1
	group by 
	stsc_date,
	member_id,
	phmc_code
	)
    ,
	--得到处理后的品类
		t1_4 as (
		--得到拼接后的品类
		select member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,max(PROD_CATE_LEV1_NAME)  as PROD_CATE_LEV1_NAME
			,max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE				--会员开卡门店 类型
			,sum(SALE_AMT) as SALE_AMT
			,max(GROS_PROF_AMT) as GROS_PROF_AMT				--毛利
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
				max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,				--会员开卡门店 类型
				max(GROS_PROF_AMT) as GROS_PROF_AMT,				--毛利
				sum(SALE_AMT) as SALE_AMT,							--销售
				max(birthday) as birthday
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
					OPEN_PHMC_CATE,				--会员开卡门店 类型
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
					,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME
					  ,birthday
				,SALE_AMT		--销售
				,GROS_PROF_AMT			--毛利
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
	--0.4、订单数据：岳阳所有订单(20190101-20191231)；订单数据基础过滤
	/*--适用范围：3.5、4.1
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
				 t1.PHMC_TYPE AS PHMC_CATE,				--是否自营
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
                 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
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
			inner join "EXT_TMP"."YUEYANG_STORE" t1 on t.PHMC_CODE=t1.PHMC_CODE 
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 t1.PHMC_TYPE,
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.GOODS_NAME
		)
     --关联得到会员信息
	 ,t1_1 as (
			select 
			s.stsc_date,					--销售日期
			s.is_weekday,					--是否工作日
			s.AT_YEAR,						--销售年份
			S.AT_MONTH,						--销售年月
			s.member_id_final,				--最终会员号
			s.IS_MEMBER,					--是否会员
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,					--会员号
			s.PHMC_CATE,				--是否自营
			T.STAR_BUSI_MONTH,			--开业年月
			s.phmc_code,				--销售门店
			s.sale_amt,					--销售金额
			s.GROS_PROF_AMT,			--销售毛利
			s.GOODS_CODE,				--商品编码
			s.GOODS_NAME,				--商品名
			c.OPEN_PHMC_CATE,			--直营加盟开卡会员
			t.phmc_type,				--门店类型
			c.birthday,					--生日
			c.sex,						--性别
			C.create_time,				--会员创建时间
			c.create_year,				--会员创建年份
			case when c.create_year=s.at_year then 1 else 0 end as memb_type,		--会员类型（年新增和复购）
			c.come_from,				--会员渠道
			n.phmc_type_name,			--门店类型名称
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
			from t1 s
			inner join 
			(
				select 
				a.phmc_code,
				a.phmc_type,
				to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_MONTH
				from dw.dim_phmc a
			) t
			on s.phmc_code=t.phmc_code
			left join  
			( 
				select 
				customer_id,
				birthday,
				sex,
				create_time,
				to_char(create_time,'yyyy') as create_year,
				come_from,
				d.phmc_type as OPEN_PHMC_CATE  
				from ds_crm.tp_cu_customerbase c
				left join "EXT_TMP"."YUEYANG_STORE" d
				on c.store=d.phmc_code
			) c 
			on s.member_id=c.customer_id
			left join 
			(
				select DICT_NAME as phmc_type_name,dict_code
				from "DS_POS"."SYS_DICT"
				 where type_code='m_shopType' and deletelable='0'
			) n
			on t.phmc_type=n.dict_code
		)
	,
	--同人同天同门店算一次
	t1_2 as (
	select
		stsc_date,
		member_id,
		phmc_code,
		to_char(stsc_date,'YYYY') as at_year,
		--max(dict_name) as dict_name,
		--max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
		--max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
		MAX(PHMC_TYPE) As PHMC_TYPE,
		max(PHMC_CATE) as PHMC_CATE,
		max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,
		max(birthday) as birthday,
		max(sex) as sex,
		max(CREATE_YEAR) as CREA_YEAR,
		max(create_time) as create_time,
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
		--sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
		--sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
		count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
		1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
	from  t1_1
	group by 
	stsc_date,
	member_id,
	phmc_code
	)
    ,
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
			,max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE				--会员开卡门店 类型
			,sum(SALE_AMT) as SALE_AMT
			,max(GROS_PROF_AMT) as GROS_PROF_AMT				--毛利
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
				max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,				--会员开卡门店 类型
				max(GROS_PROF_AMT) as GROS_PROF_AMT,				--毛利
				sum(SALE_AMT) as SALE_AMT,							--销售
				max(birthday) as birthday
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
					OPEN_PHMC_CATE,				--会员开卡门店 类型
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME
					,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME
					  ,birthday
				,SALE_AMT		--销售
				,GROS_PROF_AMT			--毛利
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
--1、现状（大数分析）
	--1.1  总会员数、消费会员数、直营总会员数、消费会员数
	/*
	t2 as (
		select t1.member_id
			,t1.OPEN_PHMC_CATE
			,if t2.member_id is not null then 1 else 0 end as IF_CNSM_FLAG
		from t1_1 t1
		left join(
			select member_id
			from t1_0
			group by member_id
		)t2
		on t1.member_id=t2.member_id
	)
	,
	t3 as (
		SELECT OPEN_PHMC_CATE
			,COUNT(MEMBER_ID) AS MEMB_NUM
			,SUM(IF_CNSM_FLAG) AS MEMB_CNSM_NUM
		FROM t2
		GROUP BY OPEN_PHMC_CATE
	)
	SELECT * FROM t3
	*/

--2、门店现状
	/*--2.1  门店销售漏斗（20180101至今）
	--37家门店
	t2 as (
		SELECT AT_YEAR
			,COUNT(DISTINCT PHMC_CODE) AS PHMC_NUM	--门店数
			,SUM(SALE_AMT) AS TOTAL_SALE_AMT		--总销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL THEN SALE_AMT ELSE 0 END) AS MEMB_SALE_AMT		--会员销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL AND OPEN_PHMC_CATE IS NOT NULL THEN SALE_AMT ELSE 0 END) AS MEMB_OPEN_SALE_AMT		--开卡会员销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL AND OPEN_PHMC_CATE IS NOT NULL AND memb_type=1 THEN SALE_AMT ELSE 0 END) AS MEMB_OPEN_NEW_SALE_AMT		--年新增开卡会员销售
		FROM t1_1
		GROUP BY AT_YEAR
	)
	,
	--13家门店
	t3 as (
		SELECT AT_YEAR
			,COUNT(DISTINCT PHMC_CODE) AS PHMC_NUM	--门店数
			,SUM(SALE_AMT) AS TOTAL_SALE_AMT		--总销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL THEN SALE_AMT ELSE 0 END) AS MEMB_SALE_AMT		--会员销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL AND OPEN_PHMC_CATE IS NOT NULL THEN SALE_AMT ELSE 0 END) AS MEMB_OPEN_SALE_AMT		--开卡会员销售
			,SUM(CASE WHEN MEMBER_ID IS NOT NULL AND OPEN_PHMC_CATE IS NOT NULL AND memb_type=1 THEN SALE_AMT ELSE 0 END) AS MEMB_OPEN_NEW_SALE_AMT		--年新增开卡会员销售
		FROM t1_1
		WHERE PHMC_CATE='直营'
		GROUP BY AT_YEAR
	)
	SELECT * FROM t2
   /*--1.3.1 各销售会员组成类型的人均产值与频次
	,t1_2 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					max(AT_YEAR) AS  AT_YEAR,
					max(AT_MONTH) AS AT_MONTH,
					max(create_year) as create_year,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where is_member='Y' and IS_SALE_YY='Y' AND IS_YY='Y' AND SALE_PHMC_FLAG='自营' and OPEN_PHMC_FLAG='自营' 
			group by 
			stsc_date,
			member_id,
			phmc_code)

	,t1_3 as 
	(select
			AT_YEAR,
			member_id,
			max(create_year) as create_year,
			sum(sale_amt) as sale_amt,
			sum(sale_times) as sale_times
			from t1_2
			group by 
			AT_YEAR,
			member_id
	 )
		
	,  t2 as (	 
		select
		a.AT_YEAR,
		a.member_id,
		case when a.create_year=a.AT_YEAR then '01'   --是否新增
			 when b.member_id is not null then '02'   --是否复购
			 when c.member_id is NOT null and a.create_year<a.AT_YEAR then '03' ----是否历史无消费回归
			 else '04' end as memb_flag,               
		sum(a.sale_amt) as sale_amt,
		sum(a.sale_times) as sale_times
		from t1_3 a
		left join t1_3 b
		on a.AT_YEAR=b.AT_YEAR+1
		and a.member_id=b.member_id 
		left join 
		(select data_date,member_id
		from DM.FACT_MEMBER_CNT_INFO 
		where R_ALL_SONSU_TIMES=0) c
		on c.data_date=add_months(last_day(a.AT_YEAR),-1) 
		and a.member_id=c.member_id
		group by a.AT_YEAR,
				 a.member_id,
			   case when a.create_year=a.AT_YEAR then '01'   --是否新增
			 when b.member_id is not null then '02'   --是否复购
			 when c.member_id is NOT null and a.create_year<a.AT_YEAR then '03' ----是否历史无消费回归
			 else '04' end)
	
    --各类型明细	
	 select 
		AT_YEAR,
		memb_flag,
		count(*) as new_memb_qty, --消费会员数
		 sum(sale_amt)/count(*) as new_memb_avg_sale, --人均年产值
		 sum(sale_times)/count(*) as new_memb_avg_times --人均消费频次 
		from t2 
		group by  AT_YEAR,
		memb_flag
		order by 
		AT_YEAR,
		memb_flag;
	
	--汇总03,04类型
	 select 
    AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end as memb_flag_total,
    count(*) as new_memb_qty, --消费会员数
	 sum(sale_amt)/count(*) as new_memb_avg_sale, --人均年产值
	 sum(sale_times)/count(*) as new_memb_avg_times --人均消费频次 
    from t2 
    group by  AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end
    order by 
    AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end;
	*/	
   
--3、会员分析
	/*--3.1  年新增会员
	--过滤得到年新增会员
	--37家门店
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
				and OPEN_PHMC_CATE is not null
				GROUP BY AT_YEAR
					,MEMBER_ID
			)GROUP BY AT_YEAR
		)
		,
		--13家门店
		t3 as (
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
				and OPEN_PHMC_CATE='自营'
				GROUP BY AT_YEAR
					,MEMBER_ID
			)GROUP BY AT_YEAR
		)
		SELECT * FROM t2
		--SELECT * FROM t3
	*/
	
	/*--3.2  年复购会员(37家)
	--每人按年统计
	t2_0 as
	(
		select 
				 AT_YEAR,
				 MEMBER_ID,
				 sum(SALE_AMT) as SALE_AMT,
				 count(1) as sale_times
			 from t1_2
			 where OPEN_PHMC_CATE is not null
			 --and OPEN_PHMC_CATE='自营'		--13家条件
			 group by AT_YEAR,
				MEMBER_ID
	)
	,
	--关联得到年复购情况
	t2_1 as (
		select  
			 t1.AT_YEAR,				--年份
			 t2.member_id ,				--上一年是否购买
			 t1.SALE_AMT,			--销售额
			 t1.sale_times				--消费次数
		from t2_0 t1
		left join t2_0 t2
		on t1.AT_YEAR=t2.AT_YEAR+1
		and t1.member_id=t2.member_id
	)
	,
	t3_1 as (
		select	AT_YEAR				--年份
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from t2_1
		group by AT_YEAR
	)
	,
	t3_2 as ( 
		select 
			AT_YEAR,
			count(1) as total_qty --消费会员总数	
		from t2_1
		group by AT_YEAR
	 )
	,t3 as (
		select 	 
			t5.AT_YEAR,					--年份
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times 		--总消费次数
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--人均销售金额
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--人均销售次数
		from t3_2 t5
		left join t3_1 t4
		on t5.AT_YEAR=t4.AT_YEAR
	)
	select * from t3
	*/
	
	/*--3.3  会员权益(37家)
	t2 as (
		select stsc_date,
		member_id,
		phmc_code,
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利额
		1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
		from  t1_2
		WHERE OPEN_PHMC_CATE IS NOT NULL
		--AND OPEN_PHMC_CATE='自营'			--放开是13家
	)
	,
	t3 as
		(
			select member_id,
				  sum(sale_amt) as SALE_AMOUNT,
				  sum(GROS_PROF_AMT) as GROS_PROF_AMT
			from t2
			group by member_id
			having sum(sale_amt)>0
	)
	,				
    t4 as
			(
				select s.member_id
					,SALE_AMOUNT		--每单消费金额
					,GROS_PROF_AMT
					,case when SALE_AMOUNT<100 then 1
					when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
					when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
					when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
					when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
					when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
					when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
					when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
					when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
					when SALE_AMOUNT>=4600 THEN 10 end as LV 
				from t3 s
			)
	,
	t5 as (
		SELECT LV
			,count(distinct member_id) as memb_num
			,sum(GROS_PROF_AMT)	as GROS_PROF_AMT
			,sum(SALE_AMOUNT) as SALE_AMOUNT
		FROM t4 
		GROUP BY LV	
	)
	SELECT * FROM t5
			
			
	
	
	*/
	
	/*--3.4  生命周期(不需要数据源，单独运行)
	with t1 as (
		select MEMB_LIFE_CYCLE,COUNT(*)
		from "DM"."FACT_MEMBER_CNT_INFO" a
		inner join  
		( 
			select 
			customer_id,
			birthday,
			sex,
			create_time,
			to_char(create_time,'yyyy') as create_year,
			come_from
			from ds_crm.tp_cu_customerbase a
			inner join "EXT_TMP"."YUEYANG_STORE"  b
			on a.store=b.phmc_code
			where phmc_type IS NOT NULL
			--AND phmc_type='自营'
		) c 
		on a.member_id=c.customer_id
		where data_date='20191224'
		GROUP BY MEMB_LIFE_CYCLE
		order by MEMB_LIFE_CYCLE
	)
	SELECT * FROM t1
	
	*/
	
	/*--3.5、疾病分析
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
	
--4、品类分析
	/*--4.1、品类销售结构（一年）
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
		WHERE OPEN_PHMC_CATE IS NOT NULL
		--AND OPEN_PHMC_CATE='自营'			--放开是13家
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
	--select * from t2
	select * from t5 order by age,PROD_CATE_LEV1_NAME
	*/
	/*--4.2、品类趋势(三年)
		t2_0 as (
		select member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,PROD_CATE_LEV1_NAME
			,OPEN_PHMC_CATE				--会员开卡门店 类型
			,SALE_AMT
			,GROS_PROF_AMT				--毛利
			,sale_times
			,birthday
		from t1_4
		WHERE OPEN_PHMC_CATE IS NOT NULL
		--AND OPEN_PHMC_CATE='自营'			--放开是13家
		
	)
	,
	t2_1 as (
		select	AT_YEAR				--年份
			,PROD_CATE_LEV2_NAME
			,sum(sale_amt) as sale_amt   --销售额
			,case when sum(sale_amt)>0 then sum(GROS_PROF_AMT)/sum(sale_amt) else 0 end as gros_rate	--毛利率
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM		--购买会员数
			,case when count(distinct member_id)>0 then sum(sale_times)/count(distinct member_id) else 0 end as memb_avg_ordr	--人均消费频次
		from t2_0
		group by AT_YEAR
			,PROD_CATE_LEV2_NAME
	)
	,
	t2_2 as ( 
		select 
			AT_YEAR
			,sum(sale_amt) as sale_amt   --销售额
			,COUNT(DISTINCT MEMBER_ID) AS MEMB_NUM		--购买会员数
		from t2_0
		group by AT_YEAR
	 )
	,t2 as (
		select 	 
			t1.AT_YEAR,					--年份
			t1.PROD_CATE_LEV2_NAME,				
			case when t2.sale_amt>0 then t1.sale_amt/t2.sale_amt else 0 end as sale_rate,	--销售占比			
			t1.gros_rate,				--毛利率
			case when t2.MEMB_NUM >0 then t1.MEMB_NUM/t2.MEMB_NUM else 0 end as memb_rate, 		--会员渗透率
			t1.memb_avg_ordr 		--人均消费频次
		from t2_1 t1
		left join t2_2 t2
		on t1.AT_YEAR=t2.AT_YEAR
	)
	select * from t2
	*/

/*plus--门店概览
	select 
      PHMC_TYPE,
       case when open_month>'201808' then '开业1年以内'
            when open_month<='201808' AND open_month>'201608' then '开业1-3年'
            when open_month<='201608' then '开业3年以上' END AS OPEN_MONTH,
       COUNT(*)
    from "EXT_TMP"."YUEYANG_STORE" 
    GROUP BY  PHMC_TYPE,
       case when open_month>'201808' then '开业1年以内'
            when open_month<='201808' AND open_month>'201608' then '开业1-3年'
            when open_month<='201608' then '开业3年以上' END
	*/
            