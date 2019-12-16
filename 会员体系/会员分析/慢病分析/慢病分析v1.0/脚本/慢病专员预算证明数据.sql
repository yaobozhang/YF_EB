
--PART1:����Ӫ�˵õ�2019�����ŵ���������
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
	)where rn=1 and store_code is not null and create_year_day<='20191118'
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
			 '20170101')) t 
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����

)
,
--�������ˣ��õ�������Ա2019���еĶ�������
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
)

,
--�������ˣ��õ�������Ա2019����������
t0_4 as (
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
,
--STEP2:�õ�ÿ���ŵ�2019ÿ���������ݣ������Ͻ�����ԱԤ��
t2_1 as (
	select store_code,AT_MONTH,
		sum(SALE_AMT) as SALE_AMT,
		sum(GROS_PROF_AMT) as GROS_PROF_AMT
	from t0_3 
	group by store_code,AT_MONTH
	
)
--�õ�ÿ���ŵ�2019ÿ���������ݣ������Ͻ�����ԱԤ��
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



--part2:֤��2019����������Ա��2017,2018�������
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
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--�������
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and crea_year<=2017
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
		and t1.create_time>='20190101' and 	t1.create_time<='20191118'	--2019������
	)
)
,
--�õ������ŵ����л�Ա
t0_11 as(
	select memb_code member_id
		,to_char(t1.CREA_TIME,'yyyy') as crea_year	--�������
		,belong_phmc_code as phmc_code	--�����ŵ�
	from
	"DW"."FACT_MEMBER_BASE" t1
	where exists(
		select 1 from t0_5 t2
		where t1.belong_phmc_code=t2."store_code"
		and to_char(t1.CREA_TIME,'yyyy')<=2017
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
			 '20170101')) t 
		where (("STSC_DATE" >='20170101' and "STSC_DATE" <='20171118')
			or
			("STSC_DATE" >='20180101' and "STSC_DATE" <='20181118')
			or
			("STSC_DATE" >='20190101' and "STSC_DATE" <='20191118')
		)
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����

)
,
--�������ˣ��õ�������Ա2019���еĶ�������
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
)
,
--�õ�������Ա���ж���
t0_4 as (
	select t1.MEMBER_ID					--������Ա
		,t1.STSC_DATE					--��������
		,t1.PHMC_CODE					--�����ŵ����
		,t1.AT_TEAR						--���
		,t1.AT_MONTH					--�·�
		,t1.SALE_AMT					--���۶�
		,t1.GROS_PROF_AMT				--ë����
		,t2.phmc_code as store_code						--�����ŵ�
	from t0_11 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	
)
,
--�õ�ÿ�����ѻ�Ա��
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



--part3:ͬ��֤��2018�ۼ�����������Ա��2018��2019�������
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
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--�������
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and create_year_day<'20190101'
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
			 '20191130'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t 
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����

)
,
--�������ˣ��õ�������Ա2019���еĶ�������
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
)
,
--�õ�ÿ�����ѻ�Ա��
t2 as (
	select AT_MONTH
		,count(distinct member_id) as memb_num 	--��������
		,count(1) as sale_num			--���Ѵ���
		,sum(SALE_AMT) as SALE_AMT		--���۽��
	from t0_3
	group by AT_TEAR
)
select AT_TEAR,memb_num,SALE_AMT,SALE_AMT/memb_num,sale_num/memb_num from t2



--part4:֤��2018��2019��������������Ա�ֱ���2018��2019�������
--���ȣ��õ�������������
with t0_0 as (
	select member_id			--������ԱID
		,store_code				--�����ŵ�
		,create_time			--����ʱ��
		,create_year			--�������
		,create_year_month		--��������
		,create_year_day		--��������
		,id						--����ID
	from
	(
		SELECT t1.CUSTOMER_ID as member_id			--��ԱID
			,t2.store_code			--�����ŵ�
			,t1.create_time			--����ʱ��
			,to_char(t1.create_time,'yyyy') as create_year	--�������
			,to_char(t1.create_time,'yyyymm') as create_year_month	--��������
			,to_char(t1.create_time,'yyyymmdd') as create_year_day	--����������
			,row_number() OVER (partition by t1.CUSTOMER_ID ORDER BY t1.create_time asc) as rn
			,t1.ID
			,to_char(t3.CREA_TIME,'yyyy') as crea_year	--�������
		FROM "DS_ZT"."CHRONIC_PATIENT" T1
		INNER JOIN "DS_ZT"."CHRONIC_PATIENT_BASELINE" T2
		on t1.ID=t2.patient_id
		INNER JOIN "DW"."FACT_MEMBER_BASE" T3
		ON T1.CUSTOMER_ID=T3.MEMB_CODE
	)where rn=1 and store_code is not null and create_year_day<'20191125' and create_year_day>='20180101'
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
		,create_year			--�������
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
--select * from t0_1 limit 10
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
			 '20191125'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t 
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����

)
,
--�������ˣ��õ�������Ա2019���еĶ�������
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
		,t1.AT_MONTH-t2.create_year_month as create_month_diff				--�����·ݲ�
	from t0_1 t2
	inner join t0_2 t1
	on t2.MEMBER_ID=t1.MEMBER_ID
	and t2.create_year=t1.AT_TEAR					--����ʱ��
)

,
--�õ�ÿ��ÿ�������ѻ�Ա�������Ѵ�������ֵ���
t2 as (
	select AT_TEAR
		,create_month_diff
		,count(distinct member_id) as memb_num 	--��������
		,count(1) as sale_num			--���Ѵ���
		,sum(SALE_AMT) as SALE_AMT		--���۽��
	from t0_3
	where STSC_DATE>=create_time
	group by AT_TEAR
	,create_month_diff
)
select AT_TEAR,create_month_diff,memb_num,SALE_AMT,SALE_AMT/memb_num,sale_num/memb_num from t2

















