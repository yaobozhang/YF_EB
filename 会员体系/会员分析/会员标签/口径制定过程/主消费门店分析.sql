--STEP1:�õ�4�궩��
with t1 as (
			SELECT
				 t."UUID",	   								--��ϸΨһ����
				 t."STSC_DATE",  								--��������
				 t."PHMC_CODE",     							--�ŵ����
				 t."MEMBER_ID",								--��Ա����
				 case when t.member_id is not null then 'Y' else 'N' end as is_member       				 --�Ƿ��ǻ�Ա
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20150101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			GROUP BY t."UUID",								   --��ϸΨһ����											   
				 t."STSC_DATE",                                  --��������
				 t."PHMC_CODE",                                  --�ŵ����
				 t."MEMBER_ID"									 --��Ա����
		)
		
--STEP2:�õ���ʧ��Ա��ʧ��ͷ����ͬһ���ŵ깺��ĸ��ʣ���һ����Ա���ʶ���֮�����180�죬��ǰ���ŵ�һ�µĸ���
--ͬ��ͬ��ͬ�ŵ���һ��
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
--ÿ�ʵ��ҵ�ǰһ�ʵ�
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
--ͳ��
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

--STEP3:�õ��������ŵ���A��һ�ι�����A�ĸ��ʣ���һ����Աһ�ʶ���֮ǰN���������������ŵ꣬�Ҹñʶ��������ڸ��ŵ��ڵĸ���
--���ð���������һ��
,t3_1 as (
		select MEMBER_ID
			,PHMC_CODE
			,STSC_DATE
			,add_months(STSC_DATE,-5) as SIX_MONTH_AGO_DATE			--����ط������޸Ŀ�5���� 4���� 3���¡�����
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
--ÿ�ʶ��������Լ�ǰ6���¶���
,t3_2 as (
	
	select T1.MEMBER_ID						--��Ա
		,t1.rk								--�������
		,t1.PHMC_CODE as AFTER_PHMC_CODE	--��ǰ���ŵ�
		,t2.PHMC_CODE AS BEFORE_PHMC_CODE	--��ȥ6�����ŵ�
	from t3_1 t1
	left join t3_1 t2
	on t1.member_id=t2.member_id
	and t1.STSC_DATE > t2.STSC_DATE 
	and t1.SIX_MONTH_AGO_DATE<t2.STSC_DATE
	where t1.STSC_DATE>='20160701'
	
)
--ÿ�ʵ�ͳ���������ŵ�
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
--ͳ��
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























