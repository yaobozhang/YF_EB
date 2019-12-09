--会员总体分析
--代码贡献者：薛艳
--代码更新时间：20190723
--数据口径：见各自模块


--简介：会员总体分析总共分为3块：1、会员大数分析；2、会员分析；3、门店品类分析

--0、数据准备
	--0.1、会员现状口径订单数据：20140101-20190101 ；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）
		/*with t1 as (
			SELECT
				 "UUID",	   								--明细唯一编码
				 "STSC_DATE",  								--销售日期
				 "SALE_ORDR_DOC",  							--销售订单号
				 "PHMC_CODE",     							--门店编码
				 "GOODS_CODE",    							--商品编码
				 "ORDR_SALE_TIME",  							--订单销售时间
				 "MEMBER_ID",								--会员编码
				 case when member_id is not null then member_id else sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 "ORDR_SOUR_CODE",							--订单来源编码
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum("GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT",				--销售额
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190601'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20140101')) 
			GROUP BY "UUID",								   --明细唯一编码											   
				 "STSC_DATE",                                  --销售日期
				 "SALE_ORDR_DOC",                              --销售订单号
				 "PHMC_CODE",                                  --门店编码
				 "GOODS_CODE",                                 --商品编码
				 "ORDR_SALE_TIME",                             --订单销售时间
				 "MEMBER_ID")                                  --会员编码
		*/
	--0.2、会员分析-年龄性别分析：20180101-20190531；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）、门店数据过滤（收购加盟门店（目前仅过滤4000的收购门店）、上海医保、当前关停门店、开业时间在20190601后的）；
	/*	with t1 as (
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
					 sum("SALE_AMT") AS "SALE_AMT"   			--销售额
				FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					 '20190601'),
					 'PLACEHOLDER' = ('$$BeginTime$$',
					 '20180101')) 
				GROUP BY "UUID",								   --明细唯一编码											   
					 "STSC_DATE",                                  --销售日期
					 "SALE_ORDR_DOC",                              --销售订单号
					 "PHMC_CODE",                                  --门店编码
					 "GOODS_CODE",                                 --商品编码
					 "ORDR_SALE_TIME",                             --订单销售时间
					 "MEMBER_ID",								   --会员编码
					 "ORDR_SOUR_CODE"),                                  
		 
		--做门店过滤			 
		t1_1 as (
		select 
					s.stsc_date,
					s.member_id,
					s.phmc_code,
					s.sale_amt,
					s.GROS_PROF_AMT,
					s.GOODS_CODE,
					t.phmc_type,
					t.ADMS_ORG_CODE,
					t.ADMS_ORG_NAME,
					c.birthday,
					c.sex,
					C.create_time,
					c.come_from,
					n.DICT_NAME
		from t1 s
		inner join 
		(select 
					phmc_code,
					ADMS_ORG_CODE,
					ADMS_ORG_NAME,
					phmc_type
		from dw.dim_phmc) t
		on s.phmc_code=t.phmc_code
		left join  
		(select 
					customer_id,
					birthday,
					sex,
					create_time,
					come_from
		from ds_crm.tp_cu_customerbase) c 
		on s.member_id=c.customer_id
		left join 
		(
		select DICT_NAME,dict_code
		from "DS_POS"."SYS_DICT"
		 where type_code='m_shopType' and deletelable='0') n
		 on t.phmc_type=n.dict_code
		where is_member='Y'     
		and not exists
		 (
		  select 1 from dw.DIM_PHMC g1
		  where g1.PHMC_CODE = s.PHMC_CODE 
		  and 
		  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
		   g1.STAR_BUSI_TIME > '20190601' 
		   or 
		   (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
		   or CLOSE_DATE is not null
		   or company_code='4000'
		   or PROP_ATTR in ('Z02','Z07')
		   )
		 )
		),

    --做同人同天同门店算一次的处理
	    t1_2 as (
		select
					stsc_date,
					member_id,
					phmc_code,
					max(dict_name) as dict_name,
					max(ADMS_ORG_CODE) as ADMS_ORG_CODE,
					max(ADMS_ORG_NAME) as ADMS_ORG_NAME,
					MAX(PHMC_TYPE) As PHMC_TYPE,
					max(birthday) as birthday,
					max(sex) as sex,
					max(create_time) as create_time,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
		from  t1_1
		group by 
				stsc_date,
				member_id,
				phmc_code)
	*/
	--0.3、会员复购数据口径（20160101-20181231数据，订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）
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
/*--1、会员现状（大数分析）*/
	/*--1.1、得到会员总数、无消费会员数、线上消费会员数、线下消费会员数、全渠道消费会员数、近一年人均销售额、近一年人均消费次数
	
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
			group by t2.member_id),

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
			 from t4),
			 
		--计算近一年人均销售额、近一年人均消费次数	 
		t6 as (
			select 
			   member_id,
			   sum(sale_amt) as one_year_sale_amt,
			   sum(sale_times) as one_year_sale_times
			from (select
			           stsc_date,
			           member_id,
					   phmc_code,
					   sum(sale_amt) as sale_amt, 
			           1 as sale_times 
			from t1
			where stsc_date>=add_years(to_date('20190601','yyyymmdd'),-1)
			      and stsc_date<'20190601'
			group by 
			stsc_date,
			member_id,
			phmc_code)
			group by member_id)
		
		select 
			 source,
			 count(*),
			 sum(one_year_sale_amt),		--近一年总销售额
			 sum(one_year_sale_times),		--近一年总消费次数
			 avg(one_year_sale_amt),		--近一年人均销售额
			 avg(one_year_sale_times)		--近一年人均消费次数
			 from (select memb_code,CREA_TIME
		from "DW"."FACT_MEMBER_BASE"
		where CREA_TIME<'20190601') a
		left join t5
		on a.memb_code=t5.member_id
		left join t6
		on a.memb_code=t6.member_id
		group by source
			 ;
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

/*--2、会员分析*/
    /*--2.1、会员分析-订单分析*/
	/*
	
	*/
	/*--2.2、会员分析-年新增会员分析*/
	/*
	*/
    /*--2.3、会员分析-年复购会员分析(--代码未整理，可查询）*/ 
		--2.3.1、取三年数据的口径，即t1，做同人同天同门店算一次处理，增加统计指标，得到营销销售额，所属品类
	--step2:同人同天同门店算一次
	,t2 as (
		select
			stsc_date,						--日期
			member_id,						--会员ID
			phmc_code,						--门店号
			AT_TEAR,						--年份带出来
			sum(sale_amt) as sale_amt, 		--销售额
			sum(PURC_MONEY) as PURC_MONEY,	--营销销售额
			sum(GROS_PROF_AMT) as gros		--毛利额
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			phmc_code,
			AT_TEAR
	)
	,
	--step2.1:品类专用，同人同年同品类聚合
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
	--step2.2:到月专用，同人同月份算一次
	t2_2 as (
		select
			member_id,					--会员ID
			AT_TEAR,					--年份带出来
			AT_MONTH,					--月份带出来
			sum(sale_amt) as sale_amt, 	--销售额
			sum(PURC_MONEY) as PURC_MONEY,--营销销售额
			sum(GROS_PROF_AMT) as gros	--毛利额
		from t1
		where is_member='Y'
		group by 
			member_id,
			AT_TEAR,
			AT_MONTH
	)
	--step2.3:为了计算2018年11月到日专用，同人同天算一次
	,
	t2_3 as (
		select
			stsc_date,					--日期
			member_id,					--会员ID
			AT_TEAR,					--年份带出来
			sum(sale_amt) as sale_amt, 	--销售额
			sum(PURC_MONEY) as PURC_MONEY,--营销销售额
			sum(GROS_PROF_AMT) as gros	--毛利额
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			AT_TEAR
	)
	,
	--step2.4:公用数据，同人同年算一次，为了得到复购率分母
	t2_4 as
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
	--STEP3:得到会员所属门店是否收购加盟、是否新老店、是哪个分公司
	--STEP3.1.1:首先，得到会员所属门店，公用数据
	t3_1_1 as
	(
		select t1.MEMBER_ID			--会员编码
			,t1.AT_STORE			--所属门店
			,t1.MEMB_TEAR			--会员开卡年限
			,t2.ADMS_ORG_CODE		--分公司编码
			,t2.ADMS_ORG_NAME		--分公司名称
			,t2.company_code		--公司编码='4000'
			,t2.PROP_ATTR			--财务字段 in ('Z02','Z07')
		from
		(
			select t1.MEMBER_ID			--会员编码
				,t2.AT_STORE			--所属门店
				,t2.MEMB_TEAR			--开卡年限
			from
			(
				select 
					MEMBER_ID	--会员编码
				from t2_4
				group by member_id
			)t1
			left join  
			(
				select 
					customer_id,
					AT_STORE,
					to_char(create_time,'YYYY') as MEMB_TEAR			--开卡年限
				from ds_crm.tp_cu_customerbase
			)t2
			on t1.member_id=t2.customer_id
		)t1
		left join dw.DIM_PHMC t2
		on t1.AT_STORE=t2.PHMC_CODE	
	)
	,
	--STEP3.1:根据年数据判断所属门店类型
	t3_1 as
	(
		select  t1.AT_TEAR
			,t1.MEMBER_ID
			,case when t1.AT_TEAR=t2.MEMB_TEAR then 1 else 2 end as IS_old_MEMB		--1为新会员，2为老会员
			,t1.SALE_AMT
			,t1.PURC_MONEY	--营销销售额
			,t1.sale_times
			,case when left(t2.company_code,1)=4 or t2.PROP_ATTR in ('Z02','Z07') then 'SG_JM' else 'NORMAL' end as SG_JM_FLAG		--是否收购加盟
			,t2.ADMS_ORG_NAME		--分公司名称
		from t2_4 t1 
		left join t3_1_1 t2
		on t1.MEMBER_ID=t2.member_id
	)
	,
	--STEP3.2.1:到月专用，关联得到会员公司及是否收购加盟
	t3_2_1 as
	(
		select t1.member_id,				--会员ID
			t1.AT_TEAR,					--年份带出来
			t1.AT_MONTH,					--月份带出来
			t2.SG_JM_FLAG,				--是否收购加盟
			t2.ADMS_ORG_NAME			--分公司名称
		from
		(
			select
				member_id,					--会员ID
				AT_TEAR,					--年份带出来
				AT_MONTH					--月份带出来
			from t2_2
		)t1
		left join
		(
			select MEMBER_ID
				,max(SG_JM_FLAG) as SG_JM_FLAG		--是否收购加盟
				,max(ADMS_ORG_NAME) as ADMS_ORG_NAME		--分公司名称
			from t3_1 t3
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
			t1.SG_JM_FLAG,					--是否收购加盟
			t1.ADMS_ORG_NAME,				--分公司名称
			case when t2.member_id is not null then 1 else 0 end as IS_CB_FLAG		--是否复购
		from t3_2_1 t1
		left join t2_4 t2 
		on t1.member_id=t2.member_id
		and t1.AT_TEAR=t2.AT_TEAR+1
		where t1.SG_JM_FLAG='NORMAL'		--只分析非收购加盟的
	)
	,
	--只分析收购加盟的年数据
	t3_3 as 
	(
		select * from t3_1 where SG_JM_FLAG='NORMAL'		--只分析非收购加盟的
		
	)
	,
	--STEP3.4.1:为了计算2018年11月到日专用，关联得到收购加盟
	t3_4_1 as
	(
		select t1.member_id,				--会员ID
			t1.AT_TEAR,					--年份带出来
			t1.stsc_date,					--日期
			t2.SG_JM_FLAG,				--是否收购加盟
			t2.ADMS_ORG_NAME			--分公司名称
		from
		(
			select
				stsc_date,					--日期
				member_id,					--会员ID
				AT_TEAR					--年份带出来
			from t2_3
		)t1
		left join
		(
			select MEMBER_ID
				,max(SG_JM_FLAG) as SG_JM_FLAG		--是否收购加盟
				,max(ADMS_ORG_NAME) as ADMS_ORG_NAME		--分公司名称
			from t3_1 t3
			group by member_id
		)t2
		on t1.member_id=t2.member_id
	)
	,
	--到月专用，关联年数据，得到每个月的复购标识
	t3_4 as
	(
		select t1.member_id,				--会员ID
			t1.AT_TEAR,						--年份带出来
			t1.stsc_date,					--月份带出来
			t1.SG_JM_FLAG,					--是否收购加盟
			t1.ADMS_ORG_NAME,				--分公司名称
			case when t2.member_id is not null then 1 else 0 end as IS_CB_FLAG		--是否复购
		from t3_4_1 t1
		left join t2_4 t2 
		on t1.member_id=t2.member_id
		and t1.AT_TEAR=t2.AT_TEAR+1
		where t1.SG_JM_FLAG='NORMAL'		--只分析非收购加盟的
		and t1.stsc_date>='20181101' and t1.stsc_date<='20181130'
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
			from t3_1 t1
			left join t3_1 t2
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
		from t3_1
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
			from t3_1 t1
			left join t3_1 t2
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
		from t3_1
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
			,IS_old_MEMB
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_TEAR,				--年份
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--分公司名称
				 t2.IS_old_MEMB,			--是否新老会员
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t3_3 t1
			left join t3_3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
		) where member_id is not null
		group by AT_TEAR
		,ADMS_ORG_NAME
		,IS_old_MEMB
	)
	,
	--计算各年度分公司消费会员总数	  
	t5_2 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			IS_old_MEMB,
			count(1) as total_qty --消费会员总数	
		from t3_3 t3
		group by AT_TEAR
		,ADMS_ORG_NAME
		,IS_old_MEMB
	 )
	 ,
	 --得到各年度分公司复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_2 as 
	 (
		select 	 
			t5.AT_TEAR,					--年份
			t5.ADMS_ORG_NAME,				--是否收购加盟
			t5.IS_old_MEMB,
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
		and t5.IS_old_MEMB =t4.IS_old_MEMB
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
	--计算各年度消费会员总数	  
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
	 --取每天各分公司复购会员数
	t4_4 as (
			select t1.stsc_date,					--日期带出来
			t1.ADMS_ORG_NAME,				--分公司名称
			t1.AT_TEAR,
			count(1) as return_memb_num	--复购人数
			from t3_4 t1
			where IS_CB_FLAG=1
			group by stsc_date
				,ADMS_ORG_NAME
				,AT_TEAR
	)
	,
	--计算各年度分公司消费会员总数	  
	t5_4 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			count(1) as total_qty --消费会员总数	
		from t3_3 t3
		group by AT_TEAR
		,ADMS_ORG_NAME
	 )
	 ,
	 --得到各年度分公司复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_4 as 
	 (
		select 	 
			t4.stsc_date,					--年份
			t4.ADMS_ORG_NAME,				--是否收购加盟
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数
			t4.AT_TEAR
		from t4_4 t4
		left join t5_4 t5
		on t4.AT_TEAR=t5.AT_TEAR+1
		and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
	 )
	 select * from t6_4
	
		/*--2.3.2、年复购会员按照收购加盟分类分别计算各项指标*/
	
		/*--2.3.3、年复购会员各分公司分类分别计算各项指标*/
		
		/*--2.3.4、年复购会员按新老门店分类分别计算各项指标*/
		
		/*--2.3.5、年复购会员按新老门店分类分别计算各项指标*/
		
		/*--2.3.6、看年复购会员当年营销权重占比、品类占比等*/
    /*
	--目的：分析2017年复购会员与非复购会员在2018年的销售与频次情况
			--step1:查2016年销售
			with t1 as (
			SELECT
				 "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 case when member_id is not null then member_id else sale_ordr_doc end as member_id_final,
				 case when member_id is not null then 'Y' else 'N' end as is_member,
				 "ORDR_SOUR_CODE",
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",
				 sum("PHMC_SID") AS "PHMC_SID",
				 sum("GOODS_SID") AS "GOODS_SID",
				 sum("SALE_QTY") AS "SALE_QTY",
				 sum("RETAIL_PRIC") AS "RETAIL_PRIC",
				 sum("MEMB_PRIC") AS "MEMB_PRIC",
				 sum("EXET_PRIC") AS "EXET_PRIC",
				 sum("APPO_PRIC") AS "APPO_PRIC",
				 sum("ACNT_PRIC") AS "ACNT_PRIC",
				 sum("SALE_AMT") AS "SALE_AMT",
				 sum("COST_PRIC") AS "COST_PRIC",
				 sum("COST_AMT") AS "COST_AMT" 
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20170101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20160101')) 
			GROUP BY "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 "ORDR_SOUR_CODE"),
				 
			----------------------------------------------------------------
			--step2:查2017年销售
			-----复购会员数-------------（2016年消费过的，在2017年继续消费，为2017年复购）
			t2 as (
			SELECT
				 "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 case when member_id is not null then member_id else sale_ordr_doc end as member_id_final,
				 case when member_id is not null then 'Y' else 'N' end as is_member,
				 "ORDR_SOUR_CODE",
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",
				 sum("PHMC_SID") AS "PHMC_SID",
				 sum("GOODS_SID") AS "GOODS_SID",
				 sum("SALE_QTY") AS "SALE_QTY",
				 sum("RETAIL_PRIC") AS "RETAIL_PRIC",
				 sum("MEMB_PRIC") AS "MEMB_PRIC",
				 sum("EXET_PRIC") AS "EXET_PRIC",
				 sum("APPO_PRIC") AS "APPO_PRIC",
				 sum("ACNT_PRIC") AS "ACNT_PRIC",
				 sum("SALE_AMT") AS "SALE_AMT",
				 sum("COST_PRIC") AS "COST_PRIC",
				 sum("COST_AMT") AS "COST_AMT" 
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20180101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20170101')) 
			GROUP BY "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 "ORDR_SOUR_CODE"),

			--对2017年的销售做同人同天同门店算一次处理	 
			t3 as (
			 select
			stsc_date,
			member_id,
			phmc_code,
			sum(sale_amt) as sale_amt, 
			1 as sale_times 
			from t2
			where is_member='Y'
			group by 
			stsc_date,
			member_id,
			phmc_code),


			--对2017年的销售做是否是复购会员打标处理
			t3_1 as (
			select 
			stsc_date,
			member_id,
			phmc_code,
			sale_amt, 
			sale_times,
			case when member_id in (SELECT member_id FROM T1 WHERE is_member='Y' GROUP BY member_id) then 'T' ELSE 'N' END AS IS_FUGOU
			from t3),

			--查询2018年复购情况
			T3_2 AS (
			SELECT
				 "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 case when member_id is not null then member_id else sale_ordr_doc end as member_id_final,
				 case when member_id is not null then 'Y' else 'N' end as is_member,
				 "ORDR_SOUR_CODE",
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",
				 sum("PHMC_SID") AS "PHMC_SID",
				 sum("GOODS_SID") AS "GOODS_SID",
				 sum("SALE_QTY") AS "SALE_QTY",
				 sum("RETAIL_PRIC") AS "RETAIL_PRIC",
				 sum("MEMB_PRIC") AS "MEMB_PRIC",
				 sum("EXET_PRIC") AS "EXET_PRIC",
				 sum("APPO_PRIC") AS "APPO_PRIC",
				 sum("ACNT_PRIC") AS "ACNT_PRIC",
				 sum("SALE_AMT") AS "SALE_AMT",
				 sum("COST_PRIC") AS "COST_PRIC",
				 sum("COST_AMT") AS "COST_AMT" 
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20180101')) 
			GROUP BY "UUID",
				 "STSC_DATE",
				 "SALE_ORDR_DOC",
				 "PHMC_CODE",
				 "GOODS_CODE",
				 "PURC_CLAS_CODE",
				 "PROD_CATE_CODE",
				 "EXET_PRIC_TYPE_CODE",
				 "REFN_ORDR_DOC",
				 "ORDR_SALE_TIME",
				 "LOAD_TIME",
				 "MEMB_CODE",
				 "ORDR_TYPE_CODE",
				 "ORDR_CATE_CODE",
				 "MEMBER_ID",
				 "ORDR_SOUR_CODE"),

			--对2018年的销售做是否是复购会员打标处理
			t3_3 as (
			 select
			stsc_date,
			member_id,
			phmc_code,
			sum(sale_amt) as sale_amt, 
			1 as sale_times 
			from T3_2
			where is_member='Y'
			group by 
			stsc_date,
			member_id,
			phmc_code),

			--计算2017年的复购会员与非复购会员在2018年复购情况	
				t4 as (
				 select  
				 stsc_month,
				 ADMS_ORG_NAME,
				 IS_FUGOU,
				 count(*) as fugou_qty,
				 sum(SALE_AMT) as SALE_AMT,
				 sum(sale_times) as sale_times
				 from 
				 (
				 select 
				 to_char(stsc_date,'yyyy') as stsc_month,
				 ADMS_ORG_NAME,
				 IS_FUGOU,
				 t3_3.MEMBER_ID,
				 sum(SALE_AMT) as SALE_AMT,
				 sum(sale_times) as sale_times
				 from
				 t3_3 
				 inner join 
				 (select memb_code,ADMS_ORG_NAME from dw.fact_member_base a
				 inner join "DW"."DIM_PHMC" b
				 on a.OPEN1_PHMC_CODE=b.phmc_code) aa
				 on t3_3.MEMBER_ID=aa.memb_code
				 inner join 
				 (select member_id,IS_FUGOU from T3_1 GROUP BY member_id,IS_FUGOU) e
				 on t3_3.MEMBER_ID=e.member_id
				 group by  to_char(stsc_date,'yyyy'),ADMS_ORG_NAME,IS_FUGOU,
				 t3_3.MEMBER_ID) a
				group by stsc_month,ADMS_ORG_NAME,IS_FUGOU)
				  
				 
			 SELECT * FROM T4;
	 
	
	
	
	
	*/
	
	/*--2.4、会员分析-注册渠道*/
	/*
	*/
	
	/*--2.5、会员分析-年龄性别*/
	/*	
	select 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' else sex end  as  sex,              --"性别"
			case when floor(days_between(c.birthday,now())/365)<=18 then '18'
				 when floor(days_between(c.birthday,now())/365)<90 then floor(days_between(c.birthday,now())/365)||''
                 when (days_between(c.birthday,now())/365)>=90 then '90'
            else  '未知'  end as  age,                                                    --"年龄"
			count(distinct member_id)  as memb_qty                           --"会员数"
			,sum(sale_amt)/count(distinct member_id) as memb_avg_sale_amt        --"人均消费"
			,sum(sale_times) as sale_times
			,sum(sale_times)/count(distinct member_id) as memb_avg_ordr
		from  t1_2 c
		group by 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' else sex end,
            case when floor(days_between(c.birthday,now())/365)<=18 then '18'
                 when floor(days_between(c.birthday,now())/365)<90 then floor(days_between(c.birthday,now())/365)||''
                 when (days_between(c.birthday,now())/365)>=90 then '90'
                 else  '未知' end ;
    */
	/*--2.6、会员分析-疾病*/
	/*
	*/
	/*--2.7、会员分析-消费金额与频次*/
    /*
	*/
	
	
    /*--3、门店品类分析*/
    /*--3.1  品类销售结构*/
	/* 
	*/
	/*--3.2 品类趋势*/
	/*
	*/
	/*--3.3 门店分析1*/
	/*
	*/
	/*--3.4 门店分析2*/
	/*
	
	*/