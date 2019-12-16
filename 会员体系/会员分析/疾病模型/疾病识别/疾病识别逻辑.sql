--疾病识别数据
--代码贡献者：姚泊彰
--代码更新时间：20191018
--数据口径：见各自模块

--疾病识别主要有以下几步
--step1:首先，得到一年订单，按天和商品进行合并,到会员、门店、商品力度
	with t1_1 as (
			SELECT
				 t."STSC_DATE",  								--销售日期
				 t."GOODS_CODE",    							--商品编码
				 t."PHMC_CODE",
				 t."MEMBER_ID",								--会员编码
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191031'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20181031')) t 
			where member_id is not null
			and not exists(
				select 1 from "DW"."DIM_PHMC" t1
				where t.PHMC_CODE=t1.PHMC_CODE
				and t1.ADMS_ORG_CODE='1025'	--过滤掉河北新兴
			)
			GROUP BY t."STSC_DATE",                                  --销售日期
				 t."GOODS_CODE",                                 --商品编码
				 t."PHMC_CODE",
				 t."MEMBER_ID"									 --会员编码
		)
		,
		t1 as (
			SELECT STSC_DATE,
				GOODS_CODE,
				MEMBER_ID,
				SUM(SALE_AMT) AS SALE_AMT,	--销售额
				SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
			FROM t1_1
			GROUP BY STSC_DATE,
				GOODS_CODE,
				MEMBER_ID
			
		)        

/*
"EXT_TMP"."BOZHANG_MEDI_DISEASE" ("GOODS_CODE" NVARCHAR(10),
	 "GOODS_NAME" NVARCHAR(100),
	 "DISEASE_CODE_LEV1" NVARCHAR(3),
	 "DISEASE_NAME_LEV1" NVARCHAR(20),
	 "DISEASE_CODE_LEV2" NVARCHAR(5),
	 "DISEASE_NAME_LEV2" NVARCHAR(20),
	 "POINT" INT) UNLOAD PRIORITY 5 AUTO MERGE 

*/
--step2:得到疾病-药数据
--首先得到主治用药，并得到每个药的条数
	,
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
	
--step2.2 再然后，取出疾病表中有两条及以上主治用药的单品
	


--step3.1:拿step1中的数据与step2.1关联得到每个会员每天买过的疾病情况，并过滤掉疾病为空的天数
	,
	t3_1_1 as (
		select  t1.MEMBER_ID,						--会员编码
			 t1.STSC_DATE,  					--销售日期
			 t1.GOODS_CODE,    				--商品编码
			 t2.DISEASE_NAME_LEV1,			--疾病一级
			 t2.DISEASE_NAME_LEV2			--疾病二级
		from t1 
		left join t2_1 t2
		on t1.GOODS_CODE=t2.GOODS_CODE
	
	)
	,
	t3_1 as (
		select MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
		FROM t3_1_1
		WHERE  DISEASE_NAME_LEV2 is not null 
		group by MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
	)
	

--step3.2:拿step1中的数据与step2.2中的数据进行关联，每个会员每天会有若干疾病数据，按照疾病数和疾病编码进行排序取第一个疾病，过滤掉疾病为空的天数


--step4:对每个会员数据按照疾病进行汇总，（然后把该表打横作为标签，在视图标签中实现，可以结合来看实现方式）
	,
	t4 as (
		select member_id,DISEASE_NAME_LEV2,count(1) as day_num 
		from t3_1
		group by member_id,DISEASE_NAME_LEV2
	)
	
--step5:统计每个疾病中会员的相关数据
	,
	t5 as(
		SELECT t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.GROS_PROF_AMT
			,t1.NUM as sale_times
			,t2.MEMB_LIFE_CYCLE
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
					SUM(SALE_AMT) AS SALE_AMT,	--销售额
					SUM(GROS_PROF_AMT) AS GROS_PROF_AMT --毛利额
				FROM t1_1 
				GROUP BY MEMBER_ID,STSC_DATE,PHMC_CODE
			)
			group by member_id
		) t1 
		LEFT JOIN 
		(
			SELECT MEMBER_ID,MEMB_LIFE_CYCLE 
			FROM DM.FACT_MEMBER_CNT_INFO 
			WHERE DATA_DATE='20191031'
		)t2
		ON t1.member_id=t2.member_id
		LEFT JOIN t4 
		on t1.MEMBER_ID=t4.member_id
	)
	,
--STEP6:统计每个疾病关联疾病相关数据
	t6 as (
		select t1.member_id
			,t1.DISEASE_NAME_LEV2 as NAME1
			,t2.DISEASE_NAME_LEV2 as NAME2
		from t4 t1
		left join t4 t2
		on t1.member_id=t2.member_id
		and t1.DISEASE_NAME_LEV2<t2.DISEASE_NAME_LEV2
	)
	,
	t7 as (
		SELECT t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.GROS_PROF_AMT
			,t1.NUM as sale_times
			,t6.NAME1
			,t6.NAME2
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
					SUM(SALE_AMT) AS SALE_AMT,	--销售额
					SUM(GROS_PROF_AMT) AS GROS_PROF_AMT --毛利额
				FROM t1_1 
				GROUP BY MEMBER_ID,STSC_DATE,PHMC_CODE
			)
			group by member_id
		) t1 
		LEFT JOIN t6 
		on t1.MEMBER_ID=t6.member_id
	
	)
	select NAME1
		,NAME2
		,count(distinct member_id) as memb_num --会员数
		,avg(SALE_AMT) as memb_year_sale
		,avg(GROS_PROF_AMT) as memb_year_gros
		,avg(sale_times) as memb_year_times
	from t7
	group by NAME1
		,NAME2
	
	select DISEASE_NAME_LEV2
		,MEMB_LIFE_CYCLE
		,count(distinct member_id) as memb_num --会员数
		,avg(SALE_AMT) as memb_year_sale
		,avg(GROS_PROF_AMT) as memb_year_gros
		,avg(sale_times) as memb_year_times
	from t5
	group by DISEASE_NAME_LEV2
		,MEMB_LIFE_CYCLE

		

--根据视图给出生命周期，疾病数据










