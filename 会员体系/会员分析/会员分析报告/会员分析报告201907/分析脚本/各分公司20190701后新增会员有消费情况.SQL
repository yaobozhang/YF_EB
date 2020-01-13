--查看2019年下半年各分公司新增会员有消费情况
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
				 '20190701')) t 
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
			  ( --上海公司医保店或者开店时间大于20191231或者有关店时间的剔除
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
	,t3 as (
		SELECT T1.AT_YEAR
			,T1.MEMB_NUM
			,T2.MEMB_NUM AS MEMB_BUY_NUM
			,T2.sale_amt
			,T2.AVG_SALE_TIMES
			,T2.AVG_sale_amt
		FROM
		(
			select AT_YEAR
				,COUNT(MEMBER_ID) AS MEMB_NUM
			FROM
			(
			select memb_code member_id
				,to_char(crea_time,'YYYY') as at_year
			from "DW"."FACT_MEMBER_BASE"
			where crea_time>='20190701'
			)GROUP BY AT_YEAR
		)T1 
		left join T2
		ON T1.AT_YEAR=T2.AT_YEAR
	)
	,
	t4_1 as (
		select MEMBER_ID
			,sum(sale_amt) as sale_amt          --销售额
			,sum(GROS_PROF_AMT) as GROS_PROF_AMT    --毛利
			,COUNT(1) AS SALE_TIMES		--消费次数
		from t1_2
		where at_year=CREA_YEAR
		GROUP BY MEMBER_ID
	)
	,
	t4 as (
		select t1.member_id
			,t1.sale_amt
			,t2.ADMS_ORG_NAME
		from
		(
			select t1.member_id
				,t1.open_phmc_code
				,t2.sale_amt
			FROM
			(
				select memb_code member_id					--会员ID
					,to_char(crea_time,'YYYY') as at_year	--年份
					,OPEN1_PHMC_CODE as open_phmc_code		--开卡门店
				from "DW"."FACT_MEMBER_BASE"
				where crea_time>='20190701' and crea_time<'20200101'
			)t1
			left join t4_1 t2
			on t1.member_id=t2.member_id
		)t1
		inner join(
			select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				STAR_BUSI_TIME,
				phmc_type
			from dw.dim_phmc
		)t2 
		on t1.open_phmc_code=t2.phmc_code
	)
	SELECT ADMS_ORG_NAME,count(1) as memb_num,sum(case when sale_amt>0 then 1 else 0 end) as if_buy_flag FROM t4 group by ADMS_ORG_NAME