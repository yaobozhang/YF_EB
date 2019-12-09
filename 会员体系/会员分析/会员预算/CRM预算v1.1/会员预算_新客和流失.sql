--STEP1:数据维度及口径
	--STEP1.1 得到订单数据，每人每天每门店算一次
	t1 as (
		SELECT t1.MEMB_CODE as member_id,				--会员ID
			to_char(t1.CREA_TIME,'YYYY') as MEMB_CREA_TEAR		--开卡年份
			BELONG_PHMC_CODE,
			case when t2.PROP_ATTR in ('Z02','Z07') THEN '2'		--收购
				WHEN t2.PROP_ATTR in ('Z03','Z04') THEN '3' 	--加盟
				when t2.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --直营
				else 0
				end as PHMC_TYPE,			--门店类型
			left(t2.PHMC_TYPE,2) as PHMC_AMT_TYPE,			--销售店型
			t2.ADMS_ORG_NAME,					--分公司名称
			,to_char(t1.CREA_TIME,'YYYY')-t2.MOVE_YEAR as OPEN_YEAR	--开业年份
		FROM "DW"."FACT_MEMBER_BASE" t1 
		LEFT JOIN (
				select PHMC_CODE
					,PHMC_TYPE
					,PROP_ATTR	
					,ADMS_ORG_NAME	--分公司
					,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--迁址年份
				from DW.DIM_PHMC 
				where close_date is null
			) t2 
		on t1.BELONG_PHMC_CODE=t2.PHMC_CODE
		where to_char(t1.CREA_TIME,'YYYY')>
		
	)
	,

--STEP2:得到各种不同维度数据指标
	--STEP2.1 得到分公司、店型、门店类型的数据
	t2_1_1 as (
		select ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型
			,PHMC_AMT_TYPE				--销售类型
			,OPEN_YEAR					--开业时间
			,MEMB_CREA_TEAR					--年份
			,sum(SALE_AMT) AS SALE_TOTAL		--总销售
			,CASE WHEN sum(SALE_AMT)>0 THEN sum(case when is_member =1 then SALE_AMT else 0 end)/sum(SALE_AMT) END as MEMB_SALE_RATE		--会员销售占比
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
			,OPEN_YEAR			--开业时间
			,AT_TEAR                    --年份
		order by ADMS_ORG_NAME			--分公司
			,PHMC_TYPE                  --门店类型
			,OPEN_YEAR			--开业时间
			,AT_TEAR                    --年份
	)
	--select * from t2_1 limit 10
	,
	t2_1 as(
		SELECT ADMS_ORG_NAME			--分公司
			,PHMC_TYPE					--门店类型		直营、收购、加盟
			,OPEN_YEAR			--开业时间
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
	select * from t2_1
	
	
--part2:证明2017,2018,2019新增会员每天回头率情况
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
			 '20191125'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20170101')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码
	)
	,
	t1_2 as (
		SELECT t1.MEMBER_ID,				--会员ID
			t1.STSC_DATE,					--销售日期			
			t1.PHMC_CODE,					--门店编码
			t1.AT_TEAR,						--年份
			t1.AT_MONTH,					--月份
			t1.is_member,					--是否会员订单
			t1.SALE_AMT,					--销售
			t1.GROS_PROF_AMT,				--毛利
			MOVE_YEAR,
			to_char(t3.CREA_TIME,'YYYY') as MEMB_CREA_TEAR,		--开卡年份
			to_char(t3.CREA_TIME,'YYYYMMDD') as MEMB_CREA_DAY		--开卡日期
		FROM t1_1 t1 
		LEFT JOIN (
				select PHMC_CODE,PHMC_TYPE,PROP_ATTR,ADMS_ORG_NAME,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--迁址年份
				from DW.DIM_PHMC 
				where close_date is null
			) t2 
		on t1.PHMC_CODE=t2.PHMC_CODE
		INNER JOIN "DW"."FACT_MEMBER_BASE" t3
		on t1.MEMBER_ID=t3.MEMB_CODE
	)
	,
	--同人同天算一次回头
	t1 as (
		select MEMBER_ID,
			max(AT_TEAR) as AT_TEAR,
			days_between(MEMB_CREA_DAY,STSC_DATE) as buy_diff
		from t1_2
		where AT_TEAR=MEMB_CREA_TEAR
		and MOVE_YEAR<AT_TEAR
		group by MEMBER_ID,
			days_between(MEMB_CREA_DAY,STSC_DATE)
	)
	,
	--选择口径
	t2 as (
		select t1.MEMB_CODE as MEMBER_ID
			,to_char(t1.CREA_TIME,'YYYY') as MEMB_CREA_TEAR	--开卡年份
			,t2.AT_TEAR
			,case when t2.buy_diff is null then 400 else t2.buy_diff END as buy_diff
			,case when t2.rn is null then 0 else t2.rn END as rn
		from "DW"."FACT_MEMBER_BASE" t1
		left join (
			select MEMBER_ID
				,AT_TEAR
				,buy_diff
				,row_number() over(partition by member_id order by buy_diff asc) as rn
			from t1
		) t2
		on t1.MEMB_CODE=t2.MEMBER_ID
		where t1.CREA_TIME>='20170101' and t1.CREA_TIME<='20191125'
	)
	,
	--先看首单转化率情况
	t3_1 as (
		select MEMBER_ID
			,MEMB_CREA_TEAR
			,buy_diff		--消费间隔天数
			,rn				--第几次消费
		from t2 
		where rn<=1
	
	)
	,
	t3_2 as (
		select MEMB_CREA_TEAR
			,buy_diff
			,count(1) as memb_num		--会员数量
		from t3_1
		where buy_diff>=0
		group by MEMB_CREA_TEAR
			,buy_diff
	)
	,
	t3 as (
		select MEMB_CREA_TEAR
			,buy_diff
			,sum(memb_num) as memb_num --累积消费会员数
		from
		(
		select t1.MEMB_CREA_TEAR
			,t1.buy_diff
			,t2.memb_num
		from t3_2 t1
		left join t3_2 t2
		on t1.MEMB_CREA_TEAR=t2.MEMB_CREA_TEAR
		and t1.buy_diff>=t2.buy_diff
		)group by MEMB_CREA_TEAR
			,buy_diff
	)
	--select * from t3
	,
	--再看2单分布情况
	t4_1 as (
		select MEMBER_ID
			,MEMB_CREA_TEAR
			,buy_diff		--消费间隔天数
			,rn				--第几次消费
		from t2 
		where buy_diff>0
	
	)
	,
	t4 as (
		select MEMB_CREA_TEAR
			,buy_diff
			,count(distinct MEMBER_ID)
		from t4_1
		group by MEMB_CREA_TEAR
			,buy_diff
	)
	select * from t4
	












