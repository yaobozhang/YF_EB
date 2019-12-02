--STEP1:拿到4年订单
with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 t."PHMC_CODE",     							--门店编码
				 t."MEMBER_ID",								--会员编码
				 case when t.member_id is not null then 'Y' else 'N' end as is_member       				 --是否是会员
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20150101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."PHMC_CODE",                                  --门店编码
				 t."MEMBER_ID"									 --会员编码
		)
		
--STEP2:得到流失会员流失回头后在同一家门店购买的概率，即一个会员两笔订单之间相隔180天，且前后门店一致的概率
--同人同天同门店算一次
,t2_1 as (
	select MEMBER_ID
		,PHMC_CODE
		,STSC_DATE
		,row_number() OVER (PARTITION BY member_id ORDER BY stsc_date asc) rk
	from
	(
		select MEMBER_ID
			,PHMC_CODE
			,STSC_DATE
		from t1 
		where is_member='Y'
		group by MEMBER_ID
			,PHMC_CODE
			,STSC_DATE
	)
)
--每笔单找到前一笔单
,t2_2 as (
	SELECT BEFORE_PHMC_CODE
		,AFTER_PHMC_CODE
	FROM
	(
		select T1.MEMBER_ID
			,t1.PHMC_CODE as AFTER_PHMC_CODE
			,t2.PHMC_CODE AS BEFORE_PHMC_CODE
			,DAYS_BETWEEN(t1.STSC_DATE,t2.STSC_DATE) as DAY_DIFF
		from t2_1 t1
		left join t2_1 t2
		on t1.member_id=t2.member_id
		and t1.rk = t2.rk+1
	)
	where BEFORE_PHMC_CODE is not null
	and AFTER_PHMC_CODE is not null
	and DAY_DIFF>180
)
--统计
,t2 as (
	SELECT SUM(IS_SAME_PHMC) AS SAME_NUM
		,COUNT(1) AS TOTAL_NUM
		,SUM(IS_SAME_PHMC)/COUNT(1)	 AS SAME_RATE
	FROM
	(
		select case when BEFORE_PHMC_CODE=AFTER_PHMC_CODE then 1 else 0 end as IS_SAME_PHMC
		from t2_2
	)
)

--STEP3:得到主消费门店是A下一次购买还是A的概率，即一个会员一笔订单之前N个月内消费最多的门店，且该笔订单发生在该门店内的概率
--先拿半年数据试一试
,t3_1 as (
		select MEMBER_ID
			,PHMC_CODE
			,STSC_DATE
			,add_months(STSC_DATE,-5) as SIX_MONTH_AGO_DATE			--这个地方可以修改看5个月 4个月 3个月。。。
			,row_number() OVER (PARTITION BY member_id ORDER BY stsc_date asc) rk
		from
		(
			select MEMBER_ID
				,PHMC_CODE
				,STSC_DATE
			from t1 
			where is_member='Y'
			and STSC_DATE>='20160101'
			group by MEMBER_ID
				,PHMC_CODE
				,STSC_DATE
		)
)
--每笔订单关联自己前6个月订单
,t3_2 as (
	
	select T1.MEMBER_ID						--会员
		,t1.rk								--订单编号
		,t1.PHMC_CODE as AFTER_PHMC_CODE	--当前单门店
		,t2.PHMC_CODE AS BEFORE_PHMC_CODE	--过去6个月门店
	from t3_1 t1
	left join t3_1 t2
	on t1.member_id=t2.member_id
	and t1.STSC_DATE > t2.STSC_DATE 
	and t1.SIX_MONTH_AGO_DATE<t2.STSC_DATE
	where t1.STSC_DATE>='20160701'
	
)
--每笔单统计主消费门店
,t3_3 as
(
	SELECT MEMBER_ID
		,RK
		,BEFORE_PHMC_CODE
		,AFTER_PHMC_CODE
	FROM
	(
		SELECT  MEMBER_ID
			,RK
			,BEFORE_PHMC_CODE
			,AFTER_PHMC_CODE
			,row_number() OVER (PARTITION BY member_id,RK ORDER BY NUM DESC) rk_1
		FROM
		(
			SELECT MEMBER_ID
				,RK
				,BEFORE_PHMC_CODE
				,MAX(AFTER_PHMC_CODE) AS AFTER_PHMC_CODE
				,COUNT(1) AS NUM
			FROM T3_2
			WHERE BEFORE_PHMC_CODE IS NOT NULL 
			AND AFTER_PHMC_CODE IS NOT NULL
			GROUP BY MEMBER_ID
				,RK
				,BEFORE_PHMC_CODE
		)
	)
	WHERE rk_1=1
)
,
--统计
t3 as (
	SELECT SUM(IS_SAME_PHMC) AS SAME_NUM
		,COUNT(1) AS TOTAL_NUM
		,SUM(IS_SAME_PHMC)/COUNT(1)	 AS SAME_RATE
	FROM
	(
		select case when BEFORE_PHMC_CODE=AFTER_PHMC_CODE then 1 else 0 end as IS_SAME_PHMC
		from t3_3
	)
)
select * from t3























