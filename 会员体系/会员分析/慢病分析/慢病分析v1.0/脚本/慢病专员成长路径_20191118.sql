--慢病专员成长路径代码
--贡献者：姚泊彰
--时间：20191118

--STEP1、得到数据源，并进行处理
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
	)where rn=1 and store_code is not null
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
			 '20171001')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码

)
,
--订单过滤，拿到建档会员的订单数据
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
	where t1.STSC_DATE>=t2.create_year_day
	--and t2.create_year_day>='20171001'
)
--select * from t0_3 where store_code='6442' and AT_MONTH='201803'

,
--再拿到建档会员监测数据
t0_4 as (
	select t1.member_id			--建档会员ID
		,t1.store_code				--创建门店
		,t1.create_time			--创建时间
		,t1.create_year_month		--创建年月
		,t1.create_year_day		--创建到日
		,t1.id						--患者ID
		,t2.time as check_time				--监测时间
		,to_char(t2.time,'YYYYMMDD') as check_DAY	--监测日
		,to_char(t2.time,'YYYYMM') as check_MONTH	--监测月份
	from t0_1 t1 
	left join "DS_ZT"."CHRONIC_PATIENT_MEDSERVICE_RECORD" t2 
	on t1.ID=t2.patient_id
	where t2.time>=t1.create_time
)

,
--STEP2:得到每个专员开始后每个月的维度数据
t2_1 as (
	
	select t1."store_code" AS store_code		--慢病门店
	,t1."start_date" as start_date		--入职时间
	,t1.start_month	--入职月份
	,t2.months
	from t0_5 t1 
	left join 
	(
		select to_char(add_months('2017-10',Row_Number() OVER(ORDER BY code desc)-1),'YYYYMM') months
		from "DW"."BI_TEMP_COUPON_ALL" limit 26
	)t2 
	on t1.start_month<=t2.months
)
--SELECT * FROM T2_1  where worker_code='00020134'
--得到每个专员开始后每个月的建档数据
,
t2_2 as (
	select t1.store_code
		,t1.months
		,count(t2.member_id) as create_memb_num
	from t2_1 t1
	left join t0_1 t2
	on t1.store_code=t2.store_code
	and t1.months=t2.create_year_month
	group by t1.store_code
		,t1.months
)

--马上得到每个专员每个月的累积建档数
,
t2_3 as(
	select t1.store_code
		,t1.months
		,sum(t2.create_memb_num) as total_create_memb_num
	from t2_2 t1
	left join t2_2 t2
	on t1.store_code=t2.store_code
	and t1.months>=t2.months
	group by t1.store_code
	,t1.months
)
,
--STEP3: 得到消费监测数据
--得到每个专员每个月每个建档会员每天的消费情况
t3_1 as (
	select t1.store_code		--专员编码
		,t1.months				--年月
		,t2.member_id				--建档会员
		,t2.stsc_date			--年月日
		,'1' as act_type		--1表示消费
		,sum(SALE_AMT) as SALE_AMT	--销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT	--毛利额
		,count(member_id) as buy_num	--消费次数
	from t2_1 t1
	left join t0_3 t2
	on t1.store_code=t2.store_code
	and t1.months =t2.AT_MONTH
	group by t1.store_code
		,t1.months
		,t2.member_id
		,t2.stsc_date
)
,
--得到每个专员每个月每个建档会员每天的监测情况
t3_2 as (
	select t1.store_code		--专员编码
		,t1.months				--年月
		,t2.member_id				--建档会员
		,t2.check_DAY as stsc_date		--年月日
		,'2' as act_type		--2表示监测
		,count(member_id) as check_num	--监测次数
	from t2_1 t1
	left join t0_4 t2
	on t1.store_code=t2.store_code
	and t1.months =t2.check_MONTH
	group by t1.store_code
		,t1.months
		,t2.member_id
		,t2.check_DAY			--年月日

)
,
--合并监测和消费数据，得到个专员每月到店及消费情况

t3_3 as (
	--统计每个慢病专员每个月建档会员消费到店情况
	select store_code,months
		,count (distinct case when buy_num>0 then member_id end) as buy_memb_num	--消费会员人数
		,count (distinct member_id) as dd_memb_num	--到店会员人数
		,sum(buy_num) as buy_num		--总消费次数
		,sum(SALE_AMT) as SALE_AMT		--总消费金额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT	--总毛利额
	from
	(
		select store_code,months,member_id,stsc_date	--维度
			,max(SALE_AMT) as SALE_AMT				--销售额
			,max(GROS_PROF_AMT) as GROS_PROF_AMT	--毛利额
			,sum(case when act_type=1 then act_num else 0 end) as buy_num		--消费次数
			,sum(case when act_type=2 then act_num else 0 end) as check_num	--监测次数
			,max(act_num) as dd_num	--到店次数
		from
		(
			select t1.store_code		--专员编码
				,t1.months				--年月
				,t1.member_id				--建档会员
				,t1.stsc_date			--年月日
				,act_type			--动作类型
				,SALE_AMT			--销售额
				,GROS_PROF_AMT		--毛利额
				,buy_num as act_num			--消费次数	
			from t3_1 t1
			union all 
			select t2.store_code		--专员编码
				,t2.months				--年月
				,t2.member_id				--建档会员
				,t2.stsc_date			--年月日
				,act_type
				,0 SALE_AMT	--销售额
				,0 GROS_PROF_AMT	--毛利额
				,check_num as act_num
			from t3_2 t2
		)
		group by store_code,months,member_id,stsc_date
	)group by store_code,months
)

,


--STEP4:合并所有数据，得到每个专员每个月数据
--首先得到专员服务门店数据，及门店主数据
t4_1 as (
	select t1.store_code
		,case when t2.PROP_ATTR in ('Z02','Z07') THEN '2'		--收购
				WHEN t2.PROP_ATTR in ('Z03','Z04') THEN '3' 	--加盟
				when t2.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --直营
				else 0
				end as PHMC_TYPE,			--门店类型
		left(t2.PHMC_TYPE,2) as PHMC_AMT_TYPE,			--销售店型
		t2.ADMS_ORG_NAME					--分公司名称
		,t2.MOVE_YEAR as OPEN_YEAR	--开业年份
	from
	(
			select DISTINCT store_code 
			FROM t3_3
	)t1
	left join (
		select PHMC_CODE,PHMC_TYPE,PROP_ATTR,ADMS_ORG_NAME,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--迁址年份
				from DW.DIM_PHMC 
				where close_date is null
	) t2
	on t1.store_code=t2.phmc_code
)
,
--关联各项指标
t4 as(
	select t1.store_code
		,t1.months
		,t1.create_memb_num			--当月建档会员
		,t2.total_create_memb_num		--累积建档会员
		,t3.buy_memb_num	--消费会员人数
		,t3.dd_memb_num	--到店会员人数
		,t3.buy_num			--总消费次数
		,t3.SALE_AMT		--总消费金额
		,t3.GROS_PROF_AMT	--总毛利额
		,to_char(t1.months,'YYYY')-t4.OPEN_YEAR as OPEN_YEAR_NUM	--开业时长
		,t4.PHMC_TYPE,			--门店类型
		t4.PHMC_AMT_TYPE,			--销售店型
		t4.ADMS_ORG_NAME					--分公司名称
		,row_number() OVER (partition by t1.store_code ORDER BY t1.months asc) as rn
	from t2_2 t1
	left join t2_3 t2
	on t1.store_code=t2.store_code
	and t1.months=t2.months
	left join t3_3 t3
	on t1.store_code=t3.store_code
	and t1.months=t3.months
	left join t4_1 t4
	on t1.store_code=t4.store_code
	where t2.total_create_memb_num >0
) 
,
--关联得到门店销售、消费次数、消费人数
t5 as(
	select t4.* 
		,t2.sale_TOTAL
		,t2.sale_times
		,t2.memb_num
	from t4
	left join
	(
		select PHMC_CODE,at_month
			,sum(SALE_AMT) as sale_TOTAL
			,count(member_id) as sale_times
			,count(distinct member_id) as memb_num
		from
		t0_2
		where is_member=1
		group by PHMC_CODE,at_month
	)t2
	on t4.store_code=t2.PHMC_CODE
	and t4.months=t2.at_month
	order by t4.store_code, months asc
)
,
--得到每个门店参考成长路径
t6 as(
	select store_code
		,months
		,create_memb_num			--当月建档会员
		,total_create_memb_num		--累积建档会员
		,buy_memb_num	--消费会员人数
		,dd_memb_num	--到店会员人数
		,buy_num			--总消费次数
		,SALE_AMT		--总消费金额
		,GROS_PROF_AMT	--总毛利额
		,OPEN_YEAR_NUM	--开业时长
		,PHMC_TYPE,			--门店类型
		PHMC_AMT_TYPE,			--销售店型
		ADMS_ORG_NAME					--分公司名称
		,rn
		,sale_TOTAL
		,sale_times
		,memb_num
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY create_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as create_memb_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY total_create_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as total_create_memb_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as buy_memb_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY dd_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as dd_memb_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as buy_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY SALE_AMT ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as SALE_AMT_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY GROS_PROF_AMT ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as GROS_PROF_AMT_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY OPEN_YEAR_NUM ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as OPEN_YEAR_NUM_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY PHMC_TYPE ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as PHMC_TYPE_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_TOTAL ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as sale_TOTAL_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_times ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as sale_times_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,PHMC_AMT_TYPE,rn) as memb_num_R
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY create_memb_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as create_memb_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY total_create_memb_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as total_create_memb_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_memb_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as buy_memb_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY dd_memb_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as dd_memb_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as buy_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY SALE_AMT ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as SALE_AMT_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY GROS_PROF_AMT ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as GROS_PROF_AMT_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY OPEN_YEAR_NUM ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as OPEN_YEAR_NUM_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY PHMC_TYPE ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as PHMC_TYPE_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_TOTAL ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as sale_TOTAL_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_times ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as sale_times_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_num ASC) over(PARTITION BY PHMC_AMT_TYPE,rn) as memb_num_ALL
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY create_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as create_memb_num_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY total_create_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as total_create_memb_num_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as buy_memb_num_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY dd_memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as dd_memb_num_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY buy_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as buy_num_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY SALE_AMT ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as SALE_AMT_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY GROS_PROF_AMT ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as GROS_PROF_AMT_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY OPEN_YEAR_NUM ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as OPEN_YEAR_NUM_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY PHMC_TYPE ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as PHMC_TYPE_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_TOTAL ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as sale_TOTAL_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY sale_times ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as sale_times_adms
		,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY memb_num ASC) over(PARTITION BY ADMS_ORG_NAME,rn) as memb_num_adms
	FROM t5

)
SELECT * FROM T6

