--会员总体分析
--代码贡献者：姚泊彰
--代码更新时间：20191220
--数据口径：见各自模块


--简介：岳阳商品分析总共分三块：1、商品销售分析；2、商品考核分析；3、重点品类分析

--0、数据准备
	/*--0.1、拿到品类分析数据源，到具体门店
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
	--得到处理后的品类
	t1_4 as (
	--得到拼接后的品类
	select member_id
		,AT_YEAR
		,PROD_CATE_LEV2_NAME
		,max(PROD_CATE_LEV1_NAME)  as PROD_CATE_LEV1_NAME
		,max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE				--会员开卡门店 类型
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
			max(OPEN_PHMC_CATE) as OPEN_PHMC_CATE,				--会员开卡门店 类型
			sum(SALE_AMT) as SALE_AMT,
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

--1、品类销售分析
	/*品类数据获取
	--首先，拿到品类数据
	t2 as (
		SELECT PROD_CATE_LEV2_NAME
				,sum(SALE_AMT) AS SALE_AMT					--销售额
				,SUM(GROS_PROF_AMT) as GROS_PROF_AMT		--毛利额
				,count(distinct member_id) as memb_num		--购买会员数
				,count(distinct member_id,PHMC_CODE,STSC_DATE) AS SALE_TIMES		--购买次数
			FROM t1_2
			group by PROD_CATE_LEV2_NAME
	)
	,
	--然后，拿到总数据
	t3 as (
		SELECT sum(SALE_AMT) AS SALE_AMT					--销售额
			,SUM(GROS_PROF_AMT) as GROS_PROF_AMT		--毛利额
			,count(distinct member_id) as memb_num		--购买会员数
			,count(distinct member_id,PHMC_CODE,STSC_DATE) AS SALE_TIMES		--购买次数
		FROM t1_1
	)
	,
	--关联得到各项指标数据
	t4 as (
		SELECT 
	)
	--1.1、零售商导向
		--得到品类上的销售占比和毛利率
		t4 as (
			SELECT PROD_CATE_LEV2_NAME
				,sum(SALE_AMT) AS SALE_AMT
				,CASE WHEN SUM(SALE_AMT) !=0 THEN SUM(GROS_PROF_AMT)/SUM(SALE_AMT) ELSE 0 END AS GROS_RATE
			FROM t1_1
			group by PROD_CATE_LEV2_NAME
		)
		select * from t2
		
	/*--1.2、消费者导向
	
	
	*/
	/*--1.3、品类结构分析
	
	
	*/