--����רԱ�ɳ�·������
--�����ߣ�Ҧ����
--ʱ�䣺20191118

--STEP1���õ�����Դ�������д���
--���ȣ��õ�������������
with t0_0 as (
	select member_id			--������ԱID
		,store_code				--�����ŵ�
		,create_time			--����ʱ��
		,create_year_month		--��������
		,create_year_day		--��������
		,id						--����ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--��ԱID
			,t2.store_code			--�����ŵ�
			,t1.create_time			--����ʱ��
			,to_char(t1.create_time,'yyyymm') as create_year_month	--��������
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--����������
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN
		"DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
	)where rn=1 and store_code is not null
)
,
--�õ�ÿ���ŵ꿪ʼʱ��
t0_5 as (
	select PHMC_CODE "store_code"		--�����ŵ�
	,OPEN_DATE "start_date"		--������ʼʱ��
	,to_char(OPEN_DATE,'yyyy-mm') start_month	--������ʼ�·�
	from
	"EXT_TMP"."MB_STORE"

)
,
t0_1 as (
	select member_id			--������ԱID
		,store_code				--�����ŵ�
		,create_time			--����ʱ��
		,create_year_month		--��������
		,create_year_day		--��������
		,id						--����ID
	from t0_0 t1
	where exists(
		select 1 from
		t0_5 t2
		where t1.store_code=t2."store_code"
		and t1.create_time>=t2."start_date"
	)
)
,
--Ȼ�󣬵õ�������רԱάϵ�����ŵ꿪ʼÿ���½�����Ա���������������
--���õ�����
t0_2 as (
	SELECT
			 t."STSC_DATE",  								--��������
			 t."PHMC_CODE",     							--�ŵ����
			 t."MEMBER_ID",									--��Ա����
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--���
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--�·�
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --�Ƿ��ǻ�Ա
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--ë����
			 sum("SALE_AMT") AS "SALE_AMT"				--���۶�
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191118'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20171001')) t 
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����

)
,
--�������ˣ��õ�������Ա�Ķ�������
t0_3 as (
	select t1.MEMBER_ID					--������Ա
		,t1.STSC_DATE					--��������
		,t1.PHMC_CODE					--�����ŵ����
		,t1.AT_TEAR						--���
		,t1.AT_MONTH					--�·�
		,t1.SALE_AMT					--���۶�
		,t1.GROS_PROF_AMT				--ë����
		,t2.store_code						--�����ŵ�
		,t2.create_time					--����ʱ��
		,t2.create_year_month			--��������
		,t2.create_year_day				--����������
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	where t1.STSC_DATE>=t2.create_year_day
	--and t2.create_year_day>='20171001'
)
--select * from t0_3 where store_code='6442' and AT_MONTH='201803'

,
--���õ�������Ա�������
t0_4 as (
	select t1.member_id			--������ԱID
		,t1.store_code				--�����ŵ�
		,t1.create_time			--����ʱ��
		,t1.create_year_month		--��������
		,t1.create_year_day		--��������
		,t1.id						--����ID
		,t2.time as check_time				--���ʱ��
		,to_char(t2.time,'YYYYMMDD') as check_DAY	--�����
		,to_char(t2.time,'YYYYMM') as check_MONTH	--����·�
	from t0_1 t1 
	left join "DS_ZT"."CHRONIC_PATIENT_MEDSERVICE_RECORD" t2 
	on t1.ID=t2.patient_id
	where t2.time>=t1.create_time
)

,
--STEP2:�õ�ÿ��רԱ��ʼ��ÿ���µ�ά������
t2_1 as (
	
	select t1."store_code" AS store_code		--�����ŵ�
	,t1."start_date" as start_date		--��ְʱ��
	,t1.start_month	--��ְ�·�
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
--�õ�ÿ��רԱ��ʼ��ÿ���µĽ�������
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

--���ϵõ�ÿ��רԱÿ���µ��ۻ�������
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
--STEP3: �õ����Ѽ������
--�õ�ÿ��רԱÿ����ÿ��������Աÿ����������
t3_1 as (
	select t1.store_code		--רԱ����
		,t1.months				--����
		,t2.member_id				--������Ա
		,t2.stsc_date			--������
		,'1' as act_type		--1��ʾ����
		,sum(SALE_AMT) as SALE_AMT	--���۶�
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT	--ë����
		,count(member_id) as buy_num	--���Ѵ���
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
--�õ�ÿ��רԱÿ����ÿ��������Աÿ��ļ�����
t3_2 as (
	select t1.store_code		--רԱ����
		,t1.months				--����
		,t2.member_id				--������Ա
		,t2.check_DAY as stsc_date		--������
		,'2' as act_type		--2��ʾ���
		,count(member_id) as check_num	--������
	from t2_1 t1
	left join t0_4 t2
	on t1.store_code=t2.store_code
	and t1.months =t2.check_MONTH
	group by t1.store_code
		,t1.months
		,t2.member_id
		,t2.check_DAY			--������

)
,
--�ϲ������������ݣ��õ���רԱÿ�µ��꼰�������

t3_3 as (
	--ͳ��ÿ������רԱÿ���½�����Ա���ѵ������
	select store_code,months
		,count (distinct case when buy_num>0 then member_id end) as buy_memb_num	--���ѻ�Ա����
		,count (distinct member_id) as dd_memb_num	--�����Ա����
		,sum(buy_num) as buy_num		--�����Ѵ���
		,sum(SALE_AMT) as SALE_AMT		--�����ѽ��
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT	--��ë����
	from
	(
		select store_code,months,member_id,stsc_date	--ά��
			,max(SALE_AMT) as SALE_AMT				--���۶�
			,max(GROS_PROF_AMT) as GROS_PROF_AMT	--ë����
			,sum(case when act_type=1 then act_num else 0 end) as buy_num		--���Ѵ���
			,sum(case when act_type=2 then act_num else 0 end) as check_num	--������
			,max(act_num) as dd_num	--�������
		from
		(
			select t1.store_code		--רԱ����
				,t1.months				--����
				,t1.member_id				--������Ա
				,t1.stsc_date			--������
				,act_type			--��������
				,SALE_AMT			--���۶�
				,GROS_PROF_AMT		--ë����
				,buy_num as act_num			--���Ѵ���	
			from t3_1 t1
			union all 
			select t2.store_code		--רԱ����
				,t2.months				--����
				,t2.member_id				--������Ա
				,t2.stsc_date			--������
				,act_type
				,0 SALE_AMT	--���۶�
				,0 GROS_PROF_AMT	--ë����
				,check_num as act_num
			from t3_2 t2
		)
		group by store_code,months,member_id,stsc_date
	)group by store_code,months
)

,


--STEP4:�ϲ��������ݣ��õ�ÿ��רԱÿ��������
--���ȵõ�רԱ�����ŵ����ݣ����ŵ�������
t4_1 as (
	select t1.store_code
		,case when t2.PROP_ATTR in ('Z02','Z07') THEN '2'		--�չ�
				WHEN t2.PROP_ATTR in ('Z03','Z04') THEN '3' 	--����
				when t2.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --ֱӪ
				else 0
				end as PHMC_TYPE,			--�ŵ�����
		left(t2.PHMC_TYPE,2) as PHMC_AMT_TYPE,			--���۵���
		t2.ADMS_ORG_NAME					--�ֹ�˾����
		,t2.MOVE_YEAR as OPEN_YEAR	--��ҵ���
	from
	(
			select DISTINCT store_code 
			FROM t3_3
	)t1
	left join (
		select PHMC_CODE,PHMC_TYPE,PROP_ATTR,ADMS_ORG_NAME,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--Ǩַ���
				from DW.DIM_PHMC 
				where close_date is null
	) t2
	on t1.store_code=t2.phmc_code
)
,
--��������ָ��
t4 as(
	select t1.store_code
		,t1.months
		,t1.create_memb_num			--���½�����Ա
		,t2.total_create_memb_num		--�ۻ�������Ա
		,t3.buy_memb_num	--���ѻ�Ա����
		,t3.dd_memb_num	--�����Ա����
		,t3.buy_num			--�����Ѵ���
		,t3.SALE_AMT		--�����ѽ��
		,t3.GROS_PROF_AMT	--��ë����
		,to_char(t1.months,'YYYY')-t4.OPEN_YEAR as OPEN_YEAR_NUM	--��ҵʱ��
		,t4.PHMC_TYPE,			--�ŵ�����
		t4.PHMC_AMT_TYPE,			--���۵���
		t4.ADMS_ORG_NAME					--�ֹ�˾����
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
--�����õ��ŵ����ۡ����Ѵ�������������
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
--�õ�ÿ���ŵ�ο��ɳ�·��
t6 as(
	select store_code
		,months
		,create_memb_num			--���½�����Ա
		,total_create_memb_num		--�ۻ�������Ա
		,buy_memb_num	--���ѻ�Ա����
		,dd_memb_num	--�����Ա����
		,buy_num			--�����Ѵ���
		,SALE_AMT		--�����ѽ��
		,GROS_PROF_AMT	--��ë����
		,OPEN_YEAR_NUM	--��ҵʱ��
		,PHMC_TYPE,			--�ŵ�����
		PHMC_AMT_TYPE,			--���۵���
		ADMS_ORG_NAME					--�ֹ�˾����
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

