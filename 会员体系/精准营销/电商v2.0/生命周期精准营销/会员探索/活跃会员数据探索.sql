--该代码为活跃会员分布分析部分
--代码贡献者：姚泊彰
--时间：20200217

--STEP1：得到会员毛利率和年产值四象限
WITH t1_0 as (
	SELECT t1.STSC_DATE,
		t1.GOODS_CODE,
		t1.MEMBER_ID,
		t2.PROD_CATE_LEV3_CODE,
		t2.PROD_CATE_LEV2_CODE,
		t2.PROD_CATE_LEV1_CODE,
		SUM(t1.SALE_AMT) AS SALE_AMT,	--销售额
		SUM(t1.GROS_PROF_AMT) AS GROS_PROF_AMT		--毛利额
	FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
		 '20200216'),
		 'PLACEHOLDER' = ('$$BeginTime$$',
		 '20190216')) t1
	LEFT JOIN dw.DIM_GOODS_H t2 on t1.goods_sid=t2.goods_sid
	WHERE MEMBER_ID IS NOT NULL
	GROUP BY t1.STSC_DATE,
		t1.GOODS_CODE,
		t1.MEMBER_ID,
		t2.PROD_CATE_LEV3_CODE,
		t2.PROD_CATE_LEV2_CODE,
		t2.PROD_CATE_LEV1_CODE
)

--STEP1.1：首先，得到近一年有消费会员生命周期信息，近一年毛利率及近一年产值
,
t1_1 as (
	select member_id		--会员ID
		,case when MEMB_LIFE_CYCLE='03' then '03' else '02' end as MEMB_LIFE_CYCLE	--生命周期
		,OFFLINE_Y_GROSS_RATE	--近一年毛利率
		,OFFLINE_Y_CNSM_AMT		--近一年产值
		,OFFLINE_Y_GROSS_RATE*OFFLINE_Y_CNSM_AMT as OFFLINE_Y_GROSS		--近一年毛利额
		,OFFLINE_Y_CNSM_TIMES	--近一年消费次数
	from "DM"."FACT_MEMBER_CNT_INFO"
	where data_date='20200216'
	and MEMB_LIFE_CYCLE in ('02','03','04')
)
--STEP1.2：匹配会员年龄，得到年龄段，性别，开卡分公司
,
t1_2 as (
	select t1.member_id		--会员ID
		,t1.MEMB_LIFE_CYCLE		--生命周期
		,t1.OFFLINE_Y_GROSS_RATE	--近一年毛利率
		,t1.OFFLINE_Y_CNSM_AMT		--近一年产值
		,t1.OFFLINE_Y_GROSS
		,t1.OFFLINE_Y_CNSM_TIMES	--近一年消费次数
		,case when age>=20 and age <90 then floor(age/10)*10 else null end as age		--年龄
		,case when MEMB_GNDR in ('男','女') then MEMB_GNDR else null end as MEMB_GNDR		--性别
		,ADMS_ORG_NAME		--所属分公司
	from
	(
		select t1.member_id		--会员ID
			,t1.MEMB_LIFE_CYCLE		--生命周期
			,t1.OFFLINE_Y_GROSS_RATE	--近一年毛利率
			,t1.OFFLINE_Y_CNSM_AMT		--近一年产值
			,t1.OFFLINE_Y_GROSS
			,t1.OFFLINE_Y_CNSM_TIMES	--近一年消费次数
			,floor(days_between(t2.birt_date,now())/365) as age		--年龄
			,t2.MEMB_GNDR		--性别
			,t2.ADMS_ORG_NAME		--所属分公司
		from t1_1 t1
		left join (
					select t1.MEMB_CODE
						,t1.birt_date
						,t1.MEMB_GNDR
						,t1.BELONG_PHMC_CODE
						,t2.ADMS_ORG_NAME
					FROM "DW"."FACT_MEMBER_BASE" t1
					LEFT JOIN dw.dim_phmc t2
					on t1.BELONG_PHMC_CODE=t2.PHMC_CODE
				) t2
		on t1.member_id=t2.MEMB_CODE
	)t1
)

--STEP1.3：然后，得到会员疾病信息，每个会员取最可能的疾病作为疾病划分依据
,
--得到疾病-药数据
--首先得到主治用药，并得到每个药的条数
t1_3_0 as (
	select GOODS_CODE,count(1) as num from "EXT_TMP"."BOZHANG_MEDI_DISEASE" where POINT=2 group by GOODS_CODE
)
--step2.1 然后，取出疾病表中只有一条主治用药的单品
,
t1_3_1 as (
	select t0.GOODS_CODE,t1.DISEASE_NAME_LEV1,t1.DISEASE_NAME_LEV2 from
	(
		select GOODS_CODE from t1_3_0 where num=1
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
t1_3_2 as (
	select MEMBER_ID
		,STSC_DATE
		,DISEASE_NAME_LEV2
	FROM (
		select  t1.MEMBER_ID,						--会员编码
		 t1.STSC_DATE,  					--销售日期
		 t1.GOODS_CODE,    				--商品编码
		 t2.DISEASE_NAME_LEV1,			--疾病一级
		 t2.DISEASE_NAME_LEV2			--疾病二级
		from t1_0 t1
		left join t1_3_1 t2
		on t1.GOODS_CODE=t2.GOODS_CODE
	)
	WHERE  DISEASE_NAME_LEV2 is not null 
	group by MEMBER_ID
		,STSC_DATE
		,DISEASE_NAME_LEV2
)
,
--对每个会员数据按照疾病进行汇总，（然后把该表打横作为标签，在视图标签中实现，可以结合来看实现方式）
t1_3 as (
	select member_id
		,DISEASE_NAME_LEV2
	from
	(
		select member_id
			,DISEASE_NAME_LEV2
			,day_num 
			,row_number() over(partition by member_id order by day_num desc) as rk
		from
		(
			select member_id,DISEASE_NAME_LEV2,count(1) as day_num 
			from t1_3_2 t2
			group by member_id,DISEASE_NAME_LEV2
		)
	)
	where rk=1
)
--STEP1.4：得到最终数据形式：会员ID，毛利率区间，年产值区间，消费频次区间，年龄，性别，开卡分公司，疾病
,
t1_4 as (
	select member_id
		,OFFLINE_Y_GROSS_RATE
		,OFFLINE_Y_CNSM_AMT
		,OFFLINE_Y_GROSS
		,OFFLINE_Y_CNSM_TIMES
		,case when OFFLINE_Y_GROSS_RATE>=gross_rate_percent_50 and OFFLINE_Y_CNSM_AMT>=sale_amt_percent_50 then 1
		when OFFLINE_Y_GROSS_RATE>=gross_rate_percent_50 and OFFLINE_Y_CNSM_AMT<sale_amt_percent_50 then 2
		when OFFLINE_Y_GROSS_RATE<gross_rate_percent_50 and OFFLINE_Y_CNSM_AMT<sale_amt_percent_50 then 3
		when OFFLINE_Y_GROSS_RATE<gross_rate_percent_50 and OFFLINE_Y_CNSM_AMT>=sale_amt_percent_50 then 4
		end as GROSS_RATE_SALE_AMT_FLAG
		,case when OFFLINE_Y_CNSM_TIMES>=TIMES_percent_50 then 1
		else 2
		end as TIMES_FLAG
		,TIMES_percent_50
		,DISEASE_NAME_LEV2
		,age
		,MEMB_GNDR
		,ADMS_ORG_NAME
	FROM
	(
		select t1.member_id
			,t1.OFFLINE_Y_GROSS_RATE
			,t1.OFFLINE_Y_CNSM_AMT
			,t1.OFFLINE_Y_GROSS
			,t1.OFFLINE_Y_CNSM_TIMES
			,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY t1.OFFLINE_Y_GROSS_RATE ASC) over() as gross_rate_percent_50								--毛利率50%
			,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY t1.OFFLINE_Y_CNSM_AMT ASC) over() as sale_amt_percent_50							--年产值50%
			,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY t1.OFFLINE_Y_CNSM_TIMES ASC) over() as TIMES_percent_50								--毛利率50%
			,t1.age		--年龄
			,t1.MEMB_GNDR		--性别
			,t1.ADMS_ORG_NAME		--所属分公司
			,t2.DISEASE_NAME_LEV2
		from t1_2 t1
		left join t1_3 t2
		on t1.member_id=t2.member_id
	)
)

--STEP1.5：四象限汇总得到总数据
,
t1_5 as (
	select GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
		,count(1) as memb_num
		,sum(OFFLINE_Y_GROSS)/sum(OFFLINE_Y_CNSM_AMT) as OFFLINE_Y_GROSS_RATE
		,avg(OFFLINE_Y_CNSM_AMT) as OFFLINE_Y_CNSM_AMT
	from t1_4 
	group by GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
)
,
--STEP1.5.1:二象限汇总得到总数据
t1_5_1 as (
	select TIMES_FLAG
		,DISEASE_NAME_LEV2
		,count(1) as memb_num
		,avg(OFFLINE_Y_CNSM_TIMES) as OFFLINE_Y_CNSM_TIMES
		,avg(OFFLINE_Y_CNSM_AMT) as OFFLINE_Y_CNSM_AMT
	from t1_4 
	group by TIMES_FLAG
		,DISEASE_NAME_LEV2
)
select * from t1_5_1 order by memb_num desc
--STEP1.6：得到前十疾病每个疾病的会员画像情况及其会员数占比
--首先得到各象限前十疾病
,
t1_6_1 as (
	SELECT GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
		,rk
	FROM 
	(
		SELECT GROSS_RATE_SALE_AMT_FLAG
			,case when DISEASE_NAME_LEV2 is null then '无' else DISEASE_NAME_LEV2 end as DISEASE_NAME_LEV2
			,row_number() over(partition by GROSS_RATE_SALE_AMT_FLAG order by memb_num desc) as rk
		FROM t1_5
	)
	where rk<=10
)

,
--然后得到前十疾病排名第一的性别，年龄，地域
--先得到年龄
t1_6_2 as (
	select t1.member_id
		,t1.age
		,t1.MEMB_GNDR
		,t1.ADMS_ORG_NAME
		,t1.GROSS_RATE_SALE_AMT_FLAG
		,t1.DISEASE_NAME_LEV2
		,t2.rk
		,count(1) over(partition by t1.GROSS_RATE_SALE_AMT_FLAG,t1.DISEASE_NAME_LEV2) as total_memb_num					--统计每个象限每个疾病人数
		,count(1) over(partition by t1.GROSS_RATE_SALE_AMT_FLAG,t1.DISEASE_NAME_LEV2,t1.age) as memb_age_num					--统计每个象限每个疾病每个年龄人数
		,count(1) over(partition by t1.GROSS_RATE_SALE_AMT_FLAG,t1.DISEASE_NAME_LEV2,t1.MEMB_GNDR) as memb_GNDR_num					--统计每个象限每个疾病每个性别人数
		,count(1) over(partition by t1.GROSS_RATE_SALE_AMT_FLAG,t1.DISEASE_NAME_LEV2,t1.ADMS_ORG_NAME) as memb_adms_num					--统计每个象限每个疾病每个地域人数
	from t1_4 t1
	inner join t1_6_1 t2
	on t1.GROSS_RATE_SALE_AMT_FLAG=t2.GROSS_RATE_SALE_AMT_FLAG
	and t1.DISEASE_NAME_LEV2=t2.DISEASE_NAME_LEV2
	
)
,
--统计年龄
t1_6_3 as (
	select GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
		,age
		,age_rate
		,rn
	from
	(
		select GROSS_RATE_SALE_AMT_FLAG
			,DISEASE_NAME_LEV2
			,age
			,age_rate
			,rn
			,row_number() over(partition by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2 order by age_rate desc) as rk
		from
		(
			select GROSS_RATE_SALE_AMT_FLAG
				,DISEASE_NAME_LEV2
				,age
				,max(rk) as rn
				,max(memb_age_num)/max(total_memb_num) as age_rate
			from t1_6_2
			group by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2,age
		)
	)
	where rk=1
)
,
--统计性别
t1_6_4 as (
	select GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
		,MEMB_GNDR
		,GNDR_rate
		,rn
	from
	(
		select GROSS_RATE_SALE_AMT_FLAG
			,DISEASE_NAME_LEV2
			,MEMB_GNDR
			,GNDR_rate
			,rn
			,row_number() over(partition by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2 order by GNDR_rate desc) as rk
		from
		(
			select GROSS_RATE_SALE_AMT_FLAG
				,DISEASE_NAME_LEV2
				,MEMB_GNDR
				,max(rk) as rn
				,max(memb_GNDR_num)/max(total_memb_num) as GNDR_rate
			from t1_6_2
			group by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2,MEMB_GNDR
		)
	)
	where rk=1
	
)
,
--统计地域
t1_6_5 as (
	select GROSS_RATE_SALE_AMT_FLAG
		,DISEASE_NAME_LEV2
		,ADMS_ORG_NAME
		,ADMS_rate
		,rn
	from
	(
		select GROSS_RATE_SALE_AMT_FLAG
			,DISEASE_NAME_LEV2
			,ADMS_ORG_NAME
			,ADMS_rate
			,rn
			,row_number() over(partition by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2 order by ADMS_rate desc) as rk
		from
		(
			select GROSS_RATE_SALE_AMT_FLAG
				,DISEASE_NAME_LEV2
				,ADMS_ORG_NAME
				,max(rk) as rn
				,max(memb_adms_num)/max(total_memb_num) as ADMS_rate
			from t1_6_2
			group by GROSS_RATE_SALE_AMT_FLAG,DISEASE_NAME_LEV2,ADMS_ORG_NAME
		)
	)
	where rk=1
	
)
--select * from t1_5 order by GROSS_RATE_SALE_AMT_FLAG,memb_num desc
select * from t1_6_3 order by rn desc


--STEP2：得到会员慢病+保健品四情况
--STEP2.1：首先，得到买过慢病和保健品的会员，并打标
--慢病：品类Y0108+Y010305+Y050201+Y0107+Y010303+Y050101，前面三个糖尿，后面三个心脑
--保健品：品类参茸贵细：Y0202 大健康中药：Y0201 保健品：Y03、Y09
,
t2_1_1 as (
	SELECT MEMBER_ID,1 AS HEALTH_GOOD_FLAG
	FROM
	(
		SELECT DISTINCT MEMBER_ID
		FROM t1_0
		where PROD_CATE_LEV1_CODE in ('Y03','Y09')
		OR PROD_CATE_LEV2_CODE IN ('Y0201','Y0202')
	)
)
,
t2_1_2 as (
		SELECT MEMBER_ID,1 AS CHRONIC_GOOD_FLAG
	FROM
	(
		SELECT DISTINCT MEMBER_ID
		FROM t1_0
		where PROD_CATE_LEV2_CODE in ('Y0107','Y0108')
		OR PROD_CATE_LEV3_CODE IN ('Y010305','Y010303','Y050201','Y050101')
	)

)


--STEP2.2：其次，用1.1的数据匹配2.1的标签并匹配会员年龄，得到年龄段，性别，开卡分公司
,
t2_2 as (
	select member_id
		,MEMB_LIFE_CYCLE
		,OFFLINE_Y_CNSM_AMT
		,OFFLINE_Y_GROSS_RATE
		,age
		,MEMB_GNDR
		,ADMS_ORG_NAME
		,HEALTH_GOOD_FLAG
		,CHRONIC_GOOD_FLAG
	FROM
	(
		select t1.member_id
			,t1.MEMB_LIFE_CYCLE
			,t1.OFFLINE_Y_GROSS_RATE
			,t1.OFFLINE_Y_CNSM_AMT
			,t1.age		--年龄
			,t1.MEMB_GNDR		--性别
			,t1.ADMS_ORG_NAME		--所属分公司
			,t2.HEALTH_GOOD_FLAG	--是否保健品用户
			,t3.CHRONIC_GOOD_FLAG	--是否慢病用户
		from t1_2 t1
		left join t2_1_1 t2
		on t1.member_id=t2.member_id
		left join t2_1_2 t3
		on t1.member_id=t3.member_id
	)
)
,
--STEP2.3：得到四种情况下的会员数汇总



























