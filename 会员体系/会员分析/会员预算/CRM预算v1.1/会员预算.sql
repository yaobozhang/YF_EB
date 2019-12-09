--会员预算数据
--代码贡献者：姚泊彰
--代码更新时间：20191015
--数据口径：见各自模块


--简介：会员预算分为两部分：1、数据维度及口径；2、各维度数据展现；
--STEP1:数据维度及口径
	--STEP1.1 得到订单数据，每人每天每门店算一次
	With t1_1 as(
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",								--会员编码
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20190930'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20160101')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码
	)
	,
	t1 as (
		SELECT t1.MEMBER_ID,				--会员ID
			t1.STSC_DATE,					--销售日期			
			t1.PHMC_CODE,					--门店编码
			t1.AT_TEAR,						--年份
			t1.AT_MONTH,					--月份
			t1.is_member,					--是否会员订单
			t1.SALE_AMT,					--销售
			t1.GROS_PROF_AMT,				--毛利
			case when t2.PROP_ATTR in ('Z02','Z07') THEN '2'		--收购
				WHEN t2.PROP_ATTR in ('Z03','Z04') THEN '3' 	--加盟
				when t2.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --直营
				else 0
				end as PHMC_TYPE,			--门店类型
			left(t2.PHMC_TYPE,2) as PHMC_AMT_TYPE,			--销售店型
			t2.ADMS_ORG_NAME,								--分公司名称
			to_char(t3.CREA_TIME,'YYYY') as MEMB_CREA_TEAR		--开卡年份
		FROM t1_1 t1 
		LEFT JOIN (
				select PHMC_CODE,PHMC_TYPE,PROP_ATTR,ADMS_ORG_NAME
				from DW.DIM_PHMC 
				where close_date is null
			) t2 
		on t1.PHMC_CODE=t2.PHMC_CODE
		LEFT JOIN "DW"."FACT_MEMBER_BASE" t3
		on t1.MEMBER_ID=t3.MEMB_CODE
		
	)
	,
	/*
	--STEP1_TOOL:得到每年有消费门店
	t1_tool_1 as (
		select ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,AT_TEAR					--年份
			,PHMC_CODE
		from t1 
		group by ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,AT_TEAR					--年份
			,PHMC_CODE
	)
	,
	t1_tool_2 as (
		select ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,AT_TEAR					--年份
			,PHMC_CODE
		from t1_tool_1 t1 
		LEFT JOIN t1_tool_1 t2
		ON T1.ADMS_ORG_NAME=T2.PHMC_TYPE
		AND T1.
		group by ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,AT_TEAR					--年份
			,PHMC_CODE
	)*/

--STEP2:得到各种不同维度数据指标
	--STEP2.1 得到分公司、店型、门店类型的数据
	t2_1_1 as (
		select ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,AT_TEAR					--年份
			,sum(SALE_AMT) AS SALE_TOTAL		--总销售
			,sum(case when is_member =1 then SALE_AMT else 0 end)/sum(SALE_AMT) as MEMB_SALE_RATE		--会员销售占比
			,sum(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_NEW_TOTAL			--新会员销售额
			,count(distinct case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_NUM			--新会员消费人数
			,count(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_TIMES			--新会员消费频次
			--,							--新会员消费客单(在外一层)
			,sum(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_OLD_TOTAL			--老会员销售额
			,COUNT(distinct case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_NUM			--老会员消费人数
			,COUNT(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_TIMES			--老会员消费频次
			,COUNT(DISTINCT PHMC_CODE) AS PHMC_NUM
			--,							--老会员消费客单(在外一层)
		from t1
		group by ADMS_ORG_NAME			--分公司
			,PHMC_TYPE                  --门店类型
			,PHMC_AMT_TYPE              --销售类型
			,AT_TEAR                    --年份
		order by ADMS_ORG_NAME			--分公司
			,PHMC_TYPE                  --门店类型
			,PHMC_AMT_TYPE              --销售类型
			,AT_TEAR                    --年份
	)
	--select * from t2_1 limit 10
	,
	t2_1 as(
		SELECT ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型		直营、收购、加盟
			,PHMC_AMT_TYPE				--门店销售类型
			,AT_TEAR					--年份
			,SALE_TOTAL		--总销售
			,MEMB_SALE_RATE		--会员销售占比
			,MEMB_NEW_TOTAL			--新会员销售额
			,MEMB_NEW_NUM			--新会员消费人数
			,MEMB_NEW_TIMES			--新会员消费频次
			,CASE WHEN MEMB_NEW_NUM >0 THEN MEMB_NEW_TOTAL/(MEMB_NEW_NUM*MEMB_NEW_TIMES) ELSE 0 END AS MEMB_NEW_UNIT							--新会员消费客单(在外一层)
			,MEMB_OLD_TOTAL			--老会员销售额
			,MEMB_OLD_NUM			--老会员消费人数
			,MEMB_OLD_TIMES			--老会员消费频次
			,CASE WHEN MEMB_OLD_NUM >0 THEN MEMB_OLD_TOTAL/(MEMB_OLD_NUM*MEMB_OLD_TIMES) ELSE 0 END AS MEMB_OLD_UNIT							--老会员消费客单(在外一层)
			,PHMC_NUM
		FROM t2_1_1 t1
	
	)
	--select * from t2_1
	,
	--step2.2 得到总预算
	t2_2_1 as (
		select AT_TEAR					--年份
			,sum(SALE_AMT) AS SALE_TOTAL		--总销售
			,sum(case when is_member =1 then SALE_AMT else 0 end)/sum(SALE_AMT) as MEMB_SALE_RATE		--会员销售占比
			,sum(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_NEW_TOTAL			--新会员销售额
			,count(distinct case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_NUM			--新会员消费人数
			,count(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_TIMES			--新会员消费频次
			--,							--新会员消费客单(在外一层)
			,sum(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_OLD_TOTAL			--老会员销售额
			,COUNT(distinct case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_NUM			--老会员消费人数
			,COUNT(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_TIMES			--老会员消费频次
			,COUNT(DISTINCT PHMC_CODE) AS PHMC_NUM
			--,							--老会员消费客单(在外一层)
		from t1
		group by AT_TEAR                    --年份
		order by AT_TEAR                    --年份
	)
	,
	t2_2 as(
		SELECT AT_TEAR					--年份
			,SALE_TOTAL		--总销售
			,MEMB_SALE_RATE		--会员销售占比
			,MEMB_NEW_TOTAL			--新会员销售额
			,MEMB_NEW_NUM			--新会员消费人数
			,MEMB_NEW_TIMES			--新会员消费频次
			,CASE WHEN MEMB_NEW_NUM >0 THEN MEMB_NEW_TOTAL/(MEMB_NEW_NUM*MEMB_NEW_TIMES) ELSE 0 END AS MEMB_NEW_UNIT							--新会员消费客单(在外一层)
			,MEMB_OLD_TOTAL			--老会员销售额
			,MEMB_OLD_NUM			--老会员消费人数
			,MEMB_OLD_TIMES			--老会员消费频次
			,CASE WHEN MEMB_OLD_NUM >0 THEN MEMB_OLD_TOTAL/(MEMB_OLD_NUM*MEMB_OLD_TIMES) ELSE 0 END AS MEMB_OLD_UNIT							--老会员消费客单(在外一层)
			,PHMC_NUM
		FROM t2_2_1 t1
	)
	select * from t2_2
	
	
--STEP3:得到各生命周期参考数据
	--STEP3.1 首先，拿2018年数据得到各生命周期的首笔客单平均值
	--得到新客无消费和流失的首笔客单平均值
	select MEMB_LIFE_CYCLE,avg(SALE_AMT),sum(SALE_AMT)/sum(GROS_PROF_AMT)
	from
	(
		SELECT t1.DATA_DATE
			,t1.MEMBER_ID
			,t1.MEMB_LIFE_CYCLE
			,t2.SALE_AMT
			,t2.GROS_PROF_AMT
			,Row_Number() OVER(partition by t1.member_id ORDER BY t2.STSC_DATE asc) as rn
		FROM "DM"."FACT_MEMBER_CNT_INFO" t1
		left join 
		(
			SELECT
					 t."STSC_DATE",  								--销售日期
					 t."PHMC_CODE",     							--门店编码
					 t."MEMBER_ID",								--会员编码
					 sum("SALE_AMT") AS "SALE_AMT",				--销售额
					sum("GROS_PROF_AMT") AS "GROS_PROF_AMT"	--毛利额
				FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					 '20191231'),
					 'PLACEHOLDER' = ('$$BeginTime$$',
					 '20180101')) t 
				GROUP BY t."STSC_DATE",                                  --销售日期
					 t."PHMC_CODE",                                  --门店编码
					 t."MEMBER_ID"									 --会员编码
		) t2
		on t1.MEMBER_ID=t2.MEMBER_ID
		where t1.DATA_DATE >= '20171231' and t1.
	)
	where rn=1
	group by MEMB_LIFE_CYCLE
	--得到注册30天内的首笔客单平均值
	select avg(SALE_AMT),sum(GROS_PROF_AMT)/sum(SALE_AMT)
	from
	(
		SELECT t1.MEMB_CODE
			,t2.SALE_AMT
			,t2.GROS_PROF_AMT
			,Row_Number() OVER(partition by t1.MEMB_CODE ORDER BY t2.STSC_DATE asc) as rn
		FROM "DW"."FACT_MEMBER_BASE" t1
		left join 
		(
			SELECT
					 t."STSC_DATE",  								--销售日期
					 t."PHMC_CODE",     							--门店编码
					 t."MEMBER_ID",								--会员编码
					 sum("SALE_AMT") AS "SALE_AMT",				--销售额
					sum("GROS_PROF_AMT") AS "GROS_PROF_AMT"	--毛利额
				FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					 '20191231'),
					 'PLACEHOLDER' = ('$$BeginTime$$',
					 '20180101')) t 
				GROUP BY t."STSC_DATE",                                  --销售日期
					 t."PHMC_CODE",                                  --门店编码
					 t."MEMBER_ID"									 --会员编码
		) t2
		on t1.MEMBER_ID=t2.MEMBER_ID
		where ADD_DAYS(t1.CREA_TIME,1) <= t2.STSC_DATE and add_days(t1.CREA_TIME,30) >= t2.STSC_DATE
	)
	where rn=1
	--STEP3.2 然后，得到两个生命周期在201801到201909之间每个月的变化会员数
	select MEMB_LIFE_CYCLE,DATA_MONTH,SUM(SAME_FLAG) AS MEMB_NUM
	from
	(
		SELECT t1.DATA_DATE
			,to_char(t2.DATA_DATE,'YYYYMM') as DATA_MONTH
			,t1.MEMB_LIFE_CYCLE
			,CASE WHEN T2.MEMB_LIFE_CYCLE=T1.MEMB_LIFE_CYCLE THEN 1 ELSE 0 END AS SAME_FLAG
		FROM "DM"."FACT_MEMBER_CNT_INFO" t1
		left join "DM"."FACT_MEMBER_CNT_INFO" t2
		on t1.MEMBER_ID=t2.MEMBER_ID
		AND T1.DATA_DATE=ADD_DAYS(ADD_MONTHS(ADD_DAYS(T2.DATA_DATE,'1'),-1),-1)
		where t1.DATA_DATE >= '20171231' and t1.DATA_DATE<='20190930' AND t1.DATA_DATE=LAST_DAY(T1.DATA_DATE)
		
	)
	group by MEMB_LIFE_CYCLE,DATA_MONTH
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	