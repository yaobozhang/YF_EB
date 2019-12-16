
--PART1:根据营运得到2019慢病门店销售数据
--首先，得到慢病建档数据
with t0_0 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--会员ID
			,t2.store_code			--建档门店
			,t1.create_time			--创建时间
			,to_char(t1.create_time,'yyyymm') as create_year_month	--创建年月
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--创建年月日
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN
		"DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
	)where rn=1 and store_code is not null and create_year_day<='20191118'
)
,
--拿到每个门店开始时间
t0_5 as (
	select PHMC_CODE "store_code"		--慢病门店
	,OPEN_DATE "start_date"		--慢病开始时间
	,to_char(OPEN_DATE,'yyyy-mm') start_month	--慢病开始月份
	from
	"EXT_TMP"."MB_STORE"

)
,
t0_1 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from t0_0 t1
	where exists(
		select 1 from
		t0_5 t2
		where t1.store_code=t2."store_code"
		and t1.create_time>=t2."start_date"
	)
)
,
--然后，得到从慢病专员维系慢病门店开始每个月建档会员建档后的销售数据
--先拿到订单
t0_2 as (
	SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",									--会员编码
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191118'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20170101')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码

)
,
--订单过滤，拿到建档会员2019所有的订单数据
t0_3 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.store_code						--建档门店
		,t2.create_time					--建档时间
		,t2.create_year_month			--建档年月
		,t2.create_year_day				--建档年月日
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
)

,
--订单过滤，拿到建档会员2019建档后数据
t0_4 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.store_code						--建档门店
		,t2.create_time					--建档时间
		,t2.create_year_month			--建档年月
		,t2.create_year_day				--建档年月日
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	where t1.STSC_DATE>=t2.create_year_day
	--and t2.create_year_day>='20171001'
)
,
--STEP2:得到每个门店2019每月销售数据，用于老建档会员预算
t2_1 as (
	select store_code,AT_MONTH,
		sum(SALE_AMT) as SALE_AMT,
		sum(GROS_PROF_AMT) as GROS_PROF_AMT
	from t0_3 
	group by store_code,AT_MONTH
	
)
--得到每个门店2019每月销售数据，用于老建档会员预算
,
t2_2 as (
	select store_code,AT_MONTH,
		count(distinct member_id) as memb_num,
		sum(SALE_AMT) as SALE_AMT,
		sum(GROS_PROF_AMT) as GROS_PROF_AMT
	from t0_4
	group by store_code,AT_MONTH
)

select * from t2_2



--part2:证明2019慢病建档会员在2017,2018表现情况
--首先，得到慢病建档数据
with t0_0 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--会员ID
			,t2.store_code			--建档门店
			,t1.create_time			--创建时间
			,to_char(t1.create_time,'yyyymm') as create_year_month	--创建年月
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--创建年月日
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--创建年份
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and crea_year<=2017
)
,
--拿到每个门店开始时间
t0_5 as (
	select PHMC_CODE "store_code"		--慢病门店
	,OPEN_DATE "start_date"		--慢病开始时间
	,to_char(OPEN_DATE,'yyyy-mm') start_month	--慢病开始月份
	from
	"EXT_TMP"."MB_STORE"

)
,
t0_1 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from t0_0 t1
	where exists(
		select 1 from
		t0_5 t2
		where t1.store_code=t2."store_code"
		and t1.create_time>=t2."start_date"
		and t1.create_time>='20190101' and 	t1.create_time<='20191118'	--2019建档人
	)
)
,
--得到慢病门店所有会员
t0_11 as(
	select memb_code member_id
		,to_char(t1.CREA_TIME,'yyyy') as crea_year	--创建年份
		,belong_phmc_code as phmc_code	--所属门店
	from
	"DW"."FACT_MEMBER_BASE" t1
	where exists(
		select 1 from t0_5 t2
		where t1.belong_phmc_code=t2."store_code"
		and to_char(t1.CREA_TIME,'yyyy')<=2017
	)

)
,

--然后，得到从慢病专员维系慢病门店开始每个月建档会员建档后的销售数据
--先拿到订单
t0_2 as (
	SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",									--会员编码
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191118'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20170101')) t 
		where (("STSC_DATE" >='20170101' and "STSC_DATE" <='20171118')
			or
			("STSC_DATE" >='20180101' and "STSC_DATE" <='20181118')
			or
			("STSC_DATE" >='20190101' and "STSC_DATE" <='20191118')
		)
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码

)
,
--订单过滤，拿到建档会员2019所有的订单数据
t0_3 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.store_code						--建档门店
		,t2.create_time					--建档时间
		,t2.create_year_month			--建档年月
		,t2.create_year_day				--建档年月日
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
)
,
--得到慢病会员所有订单
t0_4 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.phmc_code as store_code						--慢病门店
	from t0_11 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	
)
,
--得到每年消费会员数
t2 as (
	select AT_TEAR
		,count(distinct member_id) as memb_num
		,count(1) as sale_num
		,sum(SALE_AMT) as SALE_AMT
	from t0_3
	group by AT_TEAR
)
,
t3 as (
	select AT_TEAR
		,count(distinct member_id) as memb_num
		,count(1) as sale_num
		,sum(SALE_AMT) as SALE_AMT
	from t0_4
	group by AT_TEAR

)
select AT_TEAR,memb_num,SALE_AMT,SALE_AMT/memb_num,sale_num/memb_num from t3



--part3:同理证明2018累计慢病建档会员在2018，2019表现情况
--首先，得到慢病建档数据
with t0_0 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--会员ID
			,t2.store_code			--建档门店
			,t1.create_time			--创建时间
			,to_char(t1.create_time,'yyyymm') as create_year_month	--创建年月
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--创建年月日
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--创建年份
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and create_year_day<'20190101'
)
,
--拿到每个门店开始时间
t0_5 as (
	select PHMC_CODE "store_code"		--慢病门店
	,OPEN_DATE "start_date"		--慢病开始时间
	,to_char(OPEN_DATE,'yyyy-mm') start_month	--慢病开始月份
	from
	"EXT_TMP"."MB_STORE"

)
,
t0_1 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from t0_0 t1
	where exists(
		select 1 from
		t0_5 t2
		where t1.store_code=t2."store_code"
		and t1.create_time>=t2."start_date"
	)
)
,

--然后，得到从慢病专员维系慢病门店开始每个月建档会员建档后的销售数据
--先拿到订单
t0_2 as (
	SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",									--会员编码
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191130'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码

)
,
--订单过滤，拿到建档会员2019所有的订单数据
t0_3 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.store_code						--建档门店
		,t2.create_time					--建档时间
		,t2.create_year_month			--建档年月
		,t2.create_year_day				--建档年月日
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
)
,
--得到每年消费会员数
t2 as (
	select AT_MONTH
		,count(distinct member_id) as memb_num 	--消费人数
		,count(1) as sale_num			--消费次数
		,sum(SALE_AMT) as SALE_AMT		--销售金额
	from t0_3
	group by AT_TEAR
)
select AT_TEAR,memb_num,SALE_AMT,SALE_AMT/memb_num,sale_num/memb_num from t2



--part4:证明2018、2019新增慢病建档会员分别在2018，2019表现情况
--首先，得到慢病建档数据
with t0_0 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year			--创建年份
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--会员ID
			,t2.store_code			--建档门店
			,t1.create_time			--创建时间
			,to_char(t1.create_time,'yyyy') as create_year	--创建年份
			,to_char(t1.create_time,'yyyymm') as create_year_month	--创建年月
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--创建年月日
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--创建年份
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and create_year_day<'20191125' and create_year_day>='20180101'
)
,
--拿到每个门店开始时间
t0_5 as (
	select PHMC_CODE "store_code"		--慢病门店
	,OPEN_DATE "start_date"		--慢病开始时间
	,to_char(OPEN_DATE,'yyyy-mm') start_month	--慢病开始月份
	from
	"EXT_TMP"."MB_STORE"

)
,
t0_1 as (
	select member_id			--建档会员ID
		,store_code				--创建门店
		,create_time			--创建时间
		,create_year			--创建年份
		,create_year_month		--创建年月
		,create_year_day		--创建到日
		,id						--患者ID
	from t0_0 t1
	where exists(
		select 1 from
		t0_5 t2
		where t1.store_code=t2."store_code"
		and t1.create_time>=t2."start_date"
	)
)
--select * from t0_1 limit 10
,

--然后，得到从慢病专员维系慢病门店开始每个月建档会员建档后的销售数据
--先拿到订单
t0_2 as (
	SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",									--会员编码
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191125'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码

)
,
--订单过滤，拿到建档会员2019所有的订单数据
t0_3 as (
	select t1.MEMBER_ID					--建档会员
		,t1.STSC_DATE					--消费日期
		,t1.PHMC_CODE					--消费门店编码
		,t1.AT_TEAR						--年份
		,t1.AT_MONTH					--月份
		,t1.SALE_AMT					--销售额
		,t1.GROS_PROF_AMT				--毛利额
		,t2.store_code						--建档门店
		,t2.create_time					--建档时间
		,t2.create_year_month			--建档年月
		,t2.create_year_day				--建档年月日
		,t1.AT_MONTH-t2.create_year_month as create_month_diff				--创建月份差
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	and t2.create_year=t1.AT_TEAR					--建档时间
)

,
--得到每年每个月消费会员数、消费次数、产值情况
t2 as (
	select AT_TEAR
		,create_month_diff
		,count(distinct member_id) as memb_num 	--消费人数
		,count(1) as sale_num			--消费次数
		,sum(SALE_AMT) as SALE_AMT		--销售金额
	from t0_3
	where STSC_DATE>=create_time
	group by AT_TEAR
	,create_month_diff
)
select AT_TEAR,create_month_diff,memb_num,SALE_AMT,SALE_AMT/memb_num,sale_num/memb_num from t2

















