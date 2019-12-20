--会员总体分析
--代码贡献者：薛艳
--代码更新时间：20190723
--数据口径：见各自模块


--简介：会员总体分析总共分为3块：1、会员大数分析；2、会员分析；3、门店品类分析

--0、数据准备
	--0.1、会员现状口径订单数据：20140101-20191219 ；订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）
		/*with t1 as (
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
			 '20191219'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20140101')) 
		GROUP BY "UUID",								   --明细唯一编码											   
			 "STSC_DATE",                                  --销售日期
			 "SALE_ORDR_DOC",                              --销售订单号
			 "PHMC_CODE",                                  --门店编码
			 "GOODS_CODE",                                 --商品编码
			 "ORDR_SOUR_CODE",
			 "ORDR_SALE_TIME",                             --订单销售时间
			 "MEMBER_ID")                                  --会员编码
		*/
	--0.2、会员分析-年龄性别分析(20180101-20190531)/2.8、会员分析-会员各级别贡献分析(20180601-20190531)
	--/:订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）、
	--门店数据过滤（收购加盟门店（目前仅过滤4000的收购门店）、上海医保、当前关停门店、开业时间在20190601后的）
	--2.9、会员分析-性别分析(20160101-20181231)
	--/3、；门店品类分析；
	/*	with t1 as (
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
				 '20191219'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20181219')) t 
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
			   g1.STAR_BUSI_TIME > '20191219' 
			   or 
			   (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
			   or CLOSE_DATE is not null
			   or PROP_ATTR in ('Z02','Z07','Z03','Z04')
			   )
			 )
			),
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
			where stsc_date>=add_years(to_date('20191219','yyyymmdd'),-1)
			      and stsc_date<'20191219'
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
		where CREA_TIME<'20191219') a
		left join t5
		on a.memb_code=t5.member_id
		left join t6
		on a.memb_code=t6.member_id
		group by source
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
    /*--2.1、会员分析-订单分析* （全局观，不做其他收购，加盟等过滤）/ 
	
	/*
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
			 '20190101'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101'))    --此处限制时间
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
			 "ORDR_SOUR_CODE")
		
			  ---会员/非会员订单数--------	 	 
    SELECT 
    COUNT(*) FROM (	 
		select member_id_final
		 from t1
		 where is_member='N'   --Y表示会员，N表示非会员
		 --AND (ORDR_SOUR_CODE='0100' OR ORDR_SOUR_CODE IS NULL
		 --)
     group by member_id_final);
	
	---会员/非会员销售额 （客单价=销售额/消费次数）
	SELECT sum(SALE_AMT)
     from t1
     where is_member='N'  --Y表示会员，N表示非会员
	*/
	/*--2.2、会员分析-年新增会员分析*/
	/*
	--提取订单数据（自行调整所需时间）
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

		--取每年新增会员（自行调整所需时间）
		t3 as (
		select to_char(CREA_TIME,'yyyymm') as CREA_TIME,memb_code
		 from "DW"."FACT_MEMBER_BASE" a
		 where CREA_TIME<'20190101'
		 and CREA_TIME>='20180101'
		 --and memb_sour not in ('SG','JM')
		 ),
		 
		 --处理订单数据
		 t4 as (
		 select 
		 member_id,
		 sum(sale_amt) as sale_amt,
		 sum(sale_times) as sale_times
		from (select
		stsc_date,
		member_id,
		phmc_code,
		sum(sale_amt) as sale_amt, 
		1 as sale_times 
		from t1
		where is_member='Y'
		group by 
		stsc_date,
		member_id,
		phmc_code)
		group by member_id
		)

		--计算所需指标
		 SELECT 
		 SUM(CASE WHEN T4.member_id IS NOT NULL THEN 1 ELSE 0 END) AS SALE_MEMB_QTY, --消费会员数
		 sum(sale_amt),--销售额
		 sum(sale_times) --消费次数
		 FROM T3 
		 LEFT JOIN T4
		 ON T3.memb_code=T4.member_id;
	*/
    /*--2.3、会员分析-年复购会员分析(--代码已整理，可查询）*/ 
		--2.3.1、取三年数据的口径，即t1，做同人同天同门店算一次处理	 ，增加统计指标，得到营销销售额，所属品类
	/*,t2 as (
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
	 --得到各年度各品类复购会员数、消费会员数、复购人均消费额、复购人均消费频次
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
	 select * from t6_3
	
	*/
	
	/*--2.4、会员分析-注册渠道*/
	/*
	-----各渠道会员来源-------------
/* select 
		--to_char(CREA_TIME,'yyyy/mm'),
		case when MEMB_SOUR in ('ALIPAY','ZFM') then '支付宝+支付宝商城'
			  when MEMB_SOUR in ('WX','WXM') then '微信+微信商城'
			  when MEMB_SOUR in ('ST') THEN '门店' 
			  else '其他'end as MEMB_SOUR
		,count(*)
		 from 
		"DW"."FACT_MEMBER_BASE"
		where 
		CREA_TIME<'20190101'      --时间自行调整
		and CREA_TIME>='20180101'
		GROUP BY --to_char(CREA_TIME,'yyyy/mm'),
		case when MEMB_SOUR in ('ALIPAY','ZFM') then '支付宝+支付宝商城'
			  when MEMB_SOUR in ('WX','WXM') then '微信+微信商城'
			  when MEMB_SOUR in ('ST') THEN '门店' 
			  else '其他'end;
	  */
	
	/*--2.5、会员分析-会员画像（总体分析，时间段偏好，品类偏好）*/
	/*	
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
					sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
					sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code),



			--计算各年龄段整体消费情况
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
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
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
							 birthday,
							 sex,
							sum(SALE_AMT) as SALE_AMT
						from
						(
							select stsc_date,					--日期
								member_id,					--会员ID
								phmc_code,					--门店号
								AT_YEAR AS AT_TEAR,					--年份带出来
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
							AT_TEAR,					--年份带出来
							birthday,
							sex,
							PROD_CATE_LEV2_NAME
					)
					group by member_id
						,AT_TEAR
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
    */
	/*--2.5.1、年龄分析之下钻分析-工作日与周末消费时间段偏好*/
	/*
	---计算工作日与周末时间段偏好
		 t3 as (
		select 
			sex,
			age,
			is_weekday_sale_hour,
			is_weekend_sale_hour,
			row_number() over(partition by sex,age order by is_weekday_sale_times desc) as rn_1,
			row_number() over(partition by sex,age order by is_weekend_sale_times desc) as rn_2
		from 
		(
		select 
			case when sex not in ('男','女') then '未知' 
				when sex  is null then '未知' else sex end  as  sex,              --"性别"
			case when floor(days_between(c.birthday,now())/365)>=20  
				and  floor(days_between(c.birthday,now())/365)<=85 then floor((floor(days_between(c.birthday,now())/365)-20)/5)   --将年龄按5岁分等
				 else  '00'  end as  age,  --"年龄"
			case when is_weekday='Y' THEN sale_hour end as is_weekday_sale_hour , 
			case when is_weekday='N' THEN sale_hour end as is_weekend_sale_hour ,                                         
			case when is_weekday='Y' THEN sum(sale_times) END as is_weekday_sale_times,
			case when is_weekday='N' THEN sum(sale_times)  end as is_weekend_sale_times,
			sum(sale_times) as sale_times
		from  
		(
		select
			stsc_date,
			member_id,
			phmc_code,
			sale_hour,
			is_weekday,
			max(birthday) as birthday,
			max(sex) as sex,
			1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
		from  t1_1
		group by 
			stsc_date,
			member_id,
			phmc_code,
			sale_hour,
			is_weekday) c
		where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
			  and sex in ('男','女')
		group by 
			case when sex not in ('男','女') then '未知' 
				 when sex  is null then '未知' 
			   else sex end,
			is_weekday,
			sale_hour,
			case when floor(days_between(c.birthday,now())/365)>=20
				and  floor(days_between(c.birthday,now())/365)<=85 then  floor((floor(days_between(c.birthday,now())/365)-20)/5)
				 else  '00'  end
		 )
		 )
		 
		 SELECT
			 sex,
			age,
			max(is_weekday_sale_hour),
			max(is_weekend_sale_hour)
		 FROM T3
		 where rn_1=1 or rn_2=1
		 group by  sex,
		age
		;
	
	
	
	/*--2.6、会员分析-疾病*/
	/*--2.5.2、年龄段下钻分析（2018001-20190531）（总体分析，时间段偏好，品类偏好，单品偏好）*/
	/*
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
				sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
				sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
				count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
				1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code),



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
			group by 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end 
			 ),
			 
			 
			---计算年龄段偏好
			 t3 as (
			select 
			age,
			sale_hour,
			row_number() over(partition by age order by sale_times desc) as rn_1
			from 
			(
			select 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
				 when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=55 then '45<d<=55'
				 when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=85 then '55<d<=85'
				 else  '其他'  end as  age,   
			sale_hour,                                         --"年龄"
			sum(sale_times) as sale_times
			from  
			(
			select
			stsc_date,
			member_id,
			phmc_code,
			sale_hour,
			max(birthday) as birthday,
			1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code,
			sale_hour) c
			where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
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
							 birthday,
							sum(SALE_AMT) as SALE_AMT
						from
						(
							select stsc_date,					--日期
								member_id,					--会员ID
								phmc_code,					--门店号
								AT_TEAR,					--年份带出来
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
							AT_TEAR,					--年份带出来
							birthday,
							PROD_CATE_LEV2_NAME
					)
					group by member_id
						,AT_TEAR
						,PROD_CATE_LEV2_NAME
						,birthday
				) ,  

			--计算各品类总次数
			t4_2 as 
			(select
			age,
			PROD_CATE_LEV2_NAME,
			sale_times,
			row_number() over(partition by age order by sale_times desc) as rn_1
			from
			(
				select 	
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
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
			select 
			age,
			 STRING_AGG(PROD_CATE_LEV2_NAME,'/') as FIVE_PROD_CATE_NAME
			 from t4_2 where rn_1<=5
			 group by  
			age
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
			select 
			GOODS_CODE,
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
			select 
			case when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=45 then '30<d<=45'
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
			select
			stsc_date,
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
			GOODS_NAME) c
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
			 where  a.rn_1=1 and b.rn_2=1 and c.rn_3=1)
			 
			 
			 
			 

			 
			--汇总t2,t3,t4,t5各计算字段
			 select t2.*,t3.sale_hour,t4.FIVE_PROD_CATE_NAME,t5.phmc_code_qty,t7.sale_times_GOODS,t7.sale_amt_GOODS,t7.GROS_GOODS_NAME from t2
			 left join t3
			 on  t2.age=t3.age
			 left join t4
			 on  t2.age=t4.age
			 left join t5
			 on  t2.age=t5.age
			 left join t7
			 on  t2.age=t7.age
			 where rn_1=1;
	
	*/
	/*--2.5.3、年龄性别明细数据（2018001-20190531）*/
	/*
	
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
					sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
					sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code),
			
	select 
		case when sex not in ('男','女') then '未知' 
			when sex  is null then '未知' else sex end  as  sex,              --"性别"
		 floor(days_between(c.birthday,now())/365)  as  age,                                            --"年龄"
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_amt)/count(distinct member_id) as memb_avg_sale_amt        --"人均消费"
		,sum(sale_times) as sale_times
		,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
	from  t1_2 c
	where floor(days_between(c.birthday,now())/365)>=20 and floor(days_between(c.birthday,now())/365)<=85
			  and sex in ('男','女')
		group by 
		case when sex not in ('男','女') then '未知' 
			when sex  is null then '未知' else sex end ,
		 floor(days_between(c.birthday,now())/365) ;
	*/
	
	
	/*--2.7、会员分析-消费金额与频次分布*/
    /*
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
					sum(PURC_MONEY) as PURC_MONEY, 	--营销销售
					sum(NO_PURC_MONEY) as NO_PURC_MONEY,	--非营销
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			group by 
			stsc_date,
			member_id,
			phmc_code),
			
     --2.7.1 消费金额分布
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
				 else '2000元以上' end; 
				
	--2.7.2 消费频次分布
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
			from t2
			--where  member_id is not null
		   group by member_id	
		) a
		group by
		case when sale_ordr_doc<4 then sale_ordr_doc||'次'
			 when sale_ordr_doc<8 then '4-7'||'次'
			 when sale_ordr_doc<13 then '8-12次'
			 when sale_ordr_doc<19 then '13-18次'
			 when sale_ordr_doc>=19 then '19次以上'
			 else sale_ordr_doc||'次' end ;
	
	--2.7.3 明细金额分布
	select 
		 round((sale_amt-20)/40)*40+20 as  sale_amt
		,count(member_id) as memb_qty
		from
		(
			select 
			member_id
			,sum(sale_amt) sale_amt
			from t2
			--where  member_id is not null
		   group by member_id	
		) a
		group by 
		 round((sale_amt-20)/40)*40+20 ;
	*/
	/*--2.8、会员分析-会员各级别贡献分析*/
	/*
	t1_2 as
			(
				select member_id,
					  sum(sale_amt) as SALE_AMOUNT,
					  sum(GROS_PROF_AMT) as GROS_PROF_AMT
				from t1_1
				group by member_id
				having sum(sale_amt)>0)
				,
				
			t1_3 as
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
				from t1_2 s
				where not exists
				(
					select 1 from ds_crm.tp_cu_customerbase c
					where s.member_id=c.customer_id
					and c.come_from in ('SG','JM')
				)
				AND exists
				(
					select 1 from ds_crm.tp_cu_customerbase c
					where s.member_id=c.customer_id
					and create_time<'2018-06-01'
				)
			)
			SELECT LV
				,sum(GROS_PROF_AMT)
				,sum(SALE_AMOUNT)
			FROM t1_3 
			GROUP BY LV
*/
    /*--2.9、会员分析-性别分析(20160101-20181231)*/
	/*
	 t1_2 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					max(AT_YEAR) AS  AT_YEAR,
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
			phmc_code),
			
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
			when sex  is null then '未知' else sex end;
	



	
	
	
    /*--3、门店品类分析*/--

	/*3.0--品类专用*/
	/*,t2_1 as (
		--得到拼接后的品类
		select member_id
		      ,birthday
             ,sex
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,PROD_CATE_LEV1_NAME
			,sum(SALE_AMT) as SALE_AMT
			,count(1) as sale_times
			,sum(GROS_PROF_AMT) as GROS_PROF_AMT
		from
		(
			select	stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
				PROD_CATE_LEV2_NAME,
				PROD_CATE_LEV1_NAME,
				 birthday,
                 sex,
				sum(SALE_AMT) as SALE_AMT,
				sum(GROS_PROF_AMT) as GROS_PROF_AMT
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
                    birthday,
                    sex,
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME,
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
	 	             WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
	 	             ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME	  
				,SALE_AMT
				,GROS_PROF_AMT
				from t1_1 s
				left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
				on S.GOODS_CODE = T.GOODS_CODE
				where is_member='Y'   --调整会员、非会员或总体
			)
			group by stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
                birthday,
                sex,
				PROD_CATE_LEV2_NAME,
				PROD_CATE_LEV1_NAME
		)
		group by member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,PROD_CATE_LEV1_NAME
			,birthday
            ,sex
	)
	*/
	
    /*--3.1  品类销售结构*/
	/* 
	--品类销售结构(自行调整源数据时间范围)(该段在PPT中分别计算总体、会员部分)
	/*
	select 	
		PROD_CATE_LEV1_NAME,
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_times) as sale_times
		,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
	from  t2_1 c
	group by 
		PROD_CATE_LEV1_NAME
	order by 
		PROD_CATE_LEV1_NAME
	*/
	
	/*--3.1.1 年龄中各品类销售占比(该段在PPT中只计算会员部分)*/
	 /*
	 ,t3 as (
	select 	
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
from  t2_1 c
group by 
PROD_CATE_LEV1_NAME,
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
order by 
	 PROD_CATE_LEV1_NAME,
	 case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
	 else  '85以上'  end)
	 
	,t4 as 
 (select 	
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
from  t2_1 c
group by 
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
order by 
	 case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
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
	 else  '85以上'  end)
	 
	 --计算每个品类在年龄段中的销售占比
	 select 
	 t3.age,
	 t3.PROD_CATE_LEV1_NAME,
     t3.sale_amt/t4.total_sale_amt         --占比
	 from t3 
	 left join t4 
	 on t3.age=t4.age
	 order by 
	  t3.age,
	 t3.PROD_CATE_LEV1_NAME;
	 */
	
	/*--3.2 品类趋势*/
   /*
   select 	
		AT_YEAR,
		PROD_CATE_LEV2_NAME,
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_times) as sale_times
		,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
	from  t2_1 c
		group by 
		AT_YEAR,
		PROD_CATE_LEV2_NAME
		order by 
		AT_YEAR,
	 PROD_CATE_LEV2_NAME; 
    /*
	
	
	*/
	/*--3.3 门店分析2*/
	/*--3.3.1 各店型新旧消费分析（以6个月开业时间为限）（平均每个月消费会员中位数、平均每个会员每月金额中位数、平均每月复购会员数中位数）*/
	/*
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
				 '20190801'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20140101')) 
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

				 
			t1_1 as (
			select 
				s.stsc_date,
				s.member_id,
				s.phmc_code,
				s.sale_amt,
				t.phmc_type,
				case when days_between(STAR_BUSI_TIME,ORDR_SALE_TIME)<=180 then 'NEW' ELSE 'OD' end as store_type
			from t1 s
			inner join 
			(select 
			phmc_code,
			ADMS_ORG_CODE,
			ADMS_ORG_NAME,
			phmc_type,
			STAR_BUSI_TIME
			from dw.dim_phmc) t
			on s.phmc_code=t.phmc_code
			where is_member='Y'     --根据不同的情形判断是否需要过滤非会员
			and not exists
			 (
			  select 1 from dw.DIM_PHMC g1
			  where g1.PHMC_CODE = s.PHMC_CODE 
			  and 
			  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
			   g1.STAR_BUSI_TIME > '20190801' 
			   or 
			   (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
			   or CLOSE_DATE is not null
			   or company_code='4000'
			   or PROP_ATTR in ('Z02','Z07')
			   )
			 )
			), 

			t1_2 as (
			select 
			to_char(data_date,'YYYYMM') AS stsc_MONTH,
			member_id,
			R_ALL_SONSU_TIMES
			from "DM"."FACT_MEMBER_CNT_INFO"
			where data_date=last_day(data_date) and R_ALL_SONSU_TIMES<>'0'
			),

			--计算每月购买人数与金额
			t1_3 as (select   PHMC_CODE
					,PHMC_TYPE
					,store_type
					,sum(memb_num)
					,sum(SALE_MEMB_AMOUNT)
					,count(1)
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
			group by PHMC_TYPE,PHMC_CODE,store_type),
			
			--计算每月复购人数
				t1_4 as (select   PHMC_CODE
						,PHMC_TYPE
						,store_type
						,sum(memb_num)
						,sum(SALE_MEMB_AMOUNT)
						,count(1)
						,sum(memb_num)/count(1) as fugou_memb_num_avg_month						--会员每月复购购买人数
						
				from
				(select PHMC_CODE
						,PHMC_TYPE
						,store_type
						,to_char(a.stsc_date,'YYYYMM') AS stsc_MONTH
						,SUM(SALE_AMT) AS SALE_MEMB_AMOUNT		--总销售金额
						,count(distinct a.member_id) as memb_num	--会员数量
						from t1_1 a
						inner join  t1_2 b
						on a.member_id=b.member_id
						and to_char(a.stsc_date,'YYYYMM')=b.stsc_MONTH
						where a.member_id is not null                         --add by xueyan 
				group by PHMC_TYPE,PHMC_CODE,store_type,to_char(stsc_date,'YYYYMM')
				) a
				group by PHMC_TYPE,PHMC_CODE,store_type)

            --计算中位数
			select 
				PHMC_TYPE,
				dict_name,
				store_type,
				max(memb_num_percent_50),
				max(memb_sale_percent_50)
				from (
				select 
				PHMC_TYPE,
				phmc_code,
				store_type,	 
				memb_num_avg_month,
				memb_sale_avg_month,
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_num_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_num_percent_50,
				PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_sale_avg_month ASC) over(partition by PHMC_TYPE,store_type) as memb_sale_percent_50
			from t1_3) a
			left join 
			(
			select * 
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0') n
			 on a.phmc_type=n.dict_code
			 group by PHMC_TYPE,dict_name,store_type
			;
	
	*/
	/*--3.3.2 各店型新增会员分析（以6个月开业时间为限）（平均每月新增会员中位数）*/
	/*
		with t7 as (
		select 
			OPEN1_PHMC_CODE,
			phmc_type,
			store_type,
			avg(memb_qty) as avg_memb_qty
		from
		(
		select 
			to_char(CREA_TIME,'yyyy/mm') as stsc_month,
			case when days_between(STAR_BUSI_TIME,CREA_TIME)<=180 then 'NEW' ELSE 'OD' end as store_type,
			OPEN1_PHMC_CODE,
			phmc_type,
			STAR_BUSI_TIME,
			count(*) as memb_qty
		 from "DW"."FACT_MEMBER_BASE" s
		 inner join 
		(select 
			phmc_code,
			ADMS_ORG_CODE,
			ADMS_ORG_NAME,
			phmc_type,
			STAR_BUSI_TIME
		from dw.dim_phmc) t
		on s.OPEN1_PHMC_CODE=t.phmc_code
		 where --CREA_TIME<'20190601'
		 --and CREA_TIME>='20160601'
		  not exists
		 (
		  select 1 from dw.DIM_PHMC g1
		  where g1.PHMC_CODE = s.OPEN1_PHMC_CODE 
		  and 
		  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
		   g1.STAR_BUSI_TIME > '20190601' 
		   or (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
		   or CLOSE_DATE is not null
		   or company_code='4000'
		   or PROP_ATTR in ('Z02','Z07')
		   )
		 )
		 group by to_char(CREA_TIME,'yyyy/mm'),case when days_between(STAR_BUSI_TIME,CREA_TIME)<=180 then 'NEW' ELSE 'OD' end,phmc_type,OPEN1_PHMC_CODE,STAR_BUSI_TIME)
		 group by phmc_type,OPEN1_PHMC_CODE,store_type)


		select 
			PHMC_TYPE,
			dict_name,
			store_type,
			max(avg_memb_qty_percent_50)
		from (
		select 
			PHMC_TYPE,
			store_type,
			OPEN1_PHMC_CODE,	 
			PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY avg_memb_qty ASC) over(partition by PHMC_TYPE, store_type) as avg_memb_qty_percent_50
		from t7) a
		left join 
		(
		select * 
		from "DS_POS"."SYS_DICT"
		 where type_code='m_shopType' and deletelable='0') n
		 on a.phmc_type=n.dict_code
		 group by PHMC_TYPE,dict_name,store_type;
	*/
	
    /*3.4 门店分析1*/
/*with t0 as (
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
	 "ORDR_SOUR_CODE"),;


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
	 '20150101'),
	 'PLACEHOLDER' = ('$$BeginTime$$',
	 '20140101')) 
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
	 
	 
t1_1 as (
select 
s.stsc_date,
s.member_id,
s.phmc_code,
s.sale_amt,
t.phmc_type
--n.dict_name
from t1 s
inner join 
(select 
phmc_code,
ADMS_ORG_CODE,
ADMS_ORG_NAME,
phmc_type
from dw.dim_phmc) t
on s.phmc_code=t.phmc_code
--where is_member='Y'     --根据不同的情形判断是否需要过滤非会员
and not exists
 (
  select 1 from dw.DIM_PHMC g1
  where g1.PHMC_CODE = s.PHMC_CODE 
  and 
  ( --上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
   --g1.STAR_BUSI_TIME > '20190601' 
   --or 
   (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
   or CLOSE_DATE is not null
   or company_code='4000'
   or PROP_ATTR in ('Z02','Z07')
   )
 )
), 

t2 as (
select
stsc_date,
member_id,
phmc_code,
--max(dict_name) as dict_name,
MAX(PHMC_TYPE) As PHMC_TYPE,
sum(sale_amt) as sale_amt, 
1 as sale_times 
from t1_1 s
group by 
stsc_date,
member_id,
phmc_code),


--select distinct PHMC_TYPE,dict_name from t2;

t3 as (
select PHMC_CODE
       ,PHMC_TYPE
		,sum(SALE_AMOUNT)/count(1) as SALE_AMOUNT_month
		,sum(SALE_MEMB_AMOUNT)/count(1) as SALE_MEMB_AMOUNT_month
		,sum(SALE_MEMB_AMOUNT/SALE_AMOUNT)/count(1) as SALE_RATE
from 
(select PHMC_CODE
        ,PHMC_TYPE
       -- ,dict_name
		,to_char(stsc_date,'YYYYMM') AS stsc_MONTH
		,SUM(SALE_AMT) AS SALE_AMOUNT
		,SUM(CASE WHEN MEMBER_ID IS NOT NULL THEN SALE_AMT ELSE 0 END) AS SALE_MEMB_AMOUNT
	from t2
	group by PHMC_CODE,PHMC_TYPE,to_char(stsc_date,'YYYYMM')
	having SUM(SALE_AMT)<>0
) a
group by PHMC_CODE,PHMC_TYPE
)


--按照分段进行门店数统计及销售占比最大最小值

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
			from t3
		)
	) a
	left join 
(
select * 
from "DS_POS"."SYS_DICT"
 where type_code='m_shopType' and deletelable='0') n
 on a.phmc_type=n.dict_code
	group by PHMC_TYPE,dict_name;

/*
	