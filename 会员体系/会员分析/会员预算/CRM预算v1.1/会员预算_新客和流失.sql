--STEP1:����ά�ȼ��ھ�
	--STEP1.1 �õ��������ݣ�ÿ��ÿ��ÿ�ŵ���һ��
	t1 as (
		SELECT t1.MEMB_CODE as member_id,				--��ԱID
			to_char(t1.CREA_TIME,'YYYY') as MEMB_CREA_TEAR		--�������
			BELONG_PHMC_CODE,
			case when t2.PROP_ATTR in ('Z02','Z07') THEN '2'		--�չ�
				WHEN t2.PROP_ATTR in ('Z03','Z04') THEN '3' 	--����
				when t2.PROP_ATTR in ('Z01','Z06','Z08') THEN '1' --ֱӪ
				else 0
				end as PHMC_TYPE,			--�ŵ�����
			left(t2.PHMC_TYPE,2) as PHMC_AMT_TYPE,			--���۵���
			t2.ADMS_ORG_NAME,					--�ֹ�˾����
			,to_char(t1.CREA_TIME,'YYYY')-t2.MOVE_YEAR as OPEN_YEAR	--��ҵ���
		FROM "DW"."FACT_MEMBER_BASE" t1 
		LEFT JOIN (
				select PHMC_CODE
					,PHMC_TYPE
					,PROP_ATTR	
					,ADMS_ORG_NAME	--�ֹ�˾
					,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--Ǩַ���
				from DW.DIM_PHMC 
				where close_date is null
			) t2 
		on t1.BELONG_PHMC_CODE=t2.PHMC_CODE
		where to_char(t1.CREA_TIME,'YYYY')>
		
	)
	,

--STEP2:�õ����ֲ�ͬά������ָ��
	--STEP2.1 �õ��ֹ�˾�����͡��ŵ����͵�����
	t2_1_1 as (
		select ADMS_ORG_NAME			--�ֹ�˾
			,PHMC_TYPE					--�ŵ�����
			,PHMC_AMT_TYPE				--��������
			,OPEN_YEAR					--��ҵʱ��
			,MEMB_CREA_TEAR					--���
			,sum(SALE_AMT) AS SALE_TOTAL		--������
			,CASE WHEN sum(SALE_AMT)>0 THEN sum(case when is_member =1 then SALE_AMT else 0 end)/sum(SALE_AMT) END as MEMB_SALE_RATE		--��Ա����ռ��
			,sum(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_NEW_TOTAL			--�»�Ա���۶�
			,count(distinct case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_NUM			--�»�Ա��������
			,count(case when MEMB_CREA_TEAR=AT_TEAR and is_member =1 then member_id end) as MEMB_NEW_TIMES			--�»�Ա����Ƶ��
			--,							--�»�Ա���ѿ͵�(����һ��)
			,sum(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then SALE_AMT end) as MEMB_OLD_TOTAL			--�ϻ�Ա���۶�
			,COUNT(distinct case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_NUM			--�ϻ�Ա��������
			,COUNT(case when MEMB_CREA_TEAR<AT_TEAR and is_member =1 then member_id end) as MEMB_OLD_TIMES			--�ϻ�Ա����Ƶ��
			,COUNT(DISTINCT PHMC_CODE) AS PHMC_NUM
			--,							--�ϻ�Ա���ѿ͵�(����һ��)
		from t1
		group by ADMS_ORG_NAME			--�ֹ�˾
			,PHMC_TYPE                  --�ŵ�����
			,OPEN_YEAR			--��ҵʱ��
			,AT_TEAR                    --���
		order by ADMS_ORG_NAME			--�ֹ�˾
			,PHMC_TYPE                  --�ŵ�����
			,OPEN_YEAR			--��ҵʱ��
			,AT_TEAR                    --���
	)
	--select * from t2_1 limit 10
	,
	t2_1 as(
		SELECT ADMS_ORG_NAME			--�ֹ�˾
			,PHMC_TYPE					--�ŵ�����		ֱӪ���չ�������
			,OPEN_YEAR			--��ҵʱ��
			,AT_TEAR					--���
			,SALE_TOTAL		--������
			,MEMB_SALE_RATE		--��Ա����ռ��
			,MEMB_NEW_TOTAL			--�»�Ա���۶�
			,MEMB_NEW_NUM			--�»�Ա��������
			,MEMB_NEW_TIMES			--�»�Ա����Ƶ��
			,CASE WHEN MEMB_NEW_NUM >0 THEN MEMB_NEW_TOTAL/(MEMB_NEW_NUM*MEMB_NEW_TIMES) ELSE 0 END AS MEMB_NEW_UNIT							--�»�Ա���ѿ͵�(����һ��)
			,MEMB_OLD_TOTAL			--�ϻ�Ա���۶� 
			,MEMB_OLD_NUM			--�ϻ�Ա��������
			,MEMB_OLD_TIMES			--�ϻ�Ա����Ƶ��
			,CASE WHEN MEMB_OLD_NUM >0 THEN MEMB_OLD_TOTAL/(MEMB_OLD_NUM*MEMB_OLD_TIMES) ELSE 0 END AS MEMB_OLD_UNIT							--�ϻ�Ա���ѿ͵�(����һ��)
			,PHMC_NUM
		FROM t2_1_1 t1
	
	)
	select * from t2_1
	
	
--part2:֤��2017,2018,2019������Աÿ���ͷ�����
--STEP1.1 �õ��������ݣ�ÿ��ÿ��ÿ�ŵ���һ��
	With t1_1 as(
		SELECT
			 t."STSC_DATE",  								--��������
			 t."PHMC_CODE",     							--�ŵ����
			 t."MEMBER_ID",								--��Ա����
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--���
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--�·�
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --�Ƿ��ǻ�Ա
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--ë����
			 sum("SALE_AMT") AS "SALE_AMT"				--���۶�
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191125'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20170101')) t 
		GROUP BY t."STSC_DATE",                                  --��������
			 t."PHMC_CODE",                                  --�ŵ����
			 t."MEMBER_ID"									 --��Ա����
	)
	,
	t1_2 as (
		SELECT t1.MEMBER_ID,				--��ԱID
			t1.STSC_DATE,					--��������			
			t1.PHMC_CODE,					--�ŵ����
			t1.AT_TEAR,						--���
			t1.AT_MONTH,					--�·�
			t1.is_member,					--�Ƿ��Ա����
			t1.SALE_AMT,					--����
			t1.GROS_PROF_AMT,				--ë��
			MOVE_YEAR,
			to_char(t3.CREA_TIME,'YYYY') as MEMB_CREA_TEAR,		--�������
			to_char(t3.CREA_TIME,'YYYYMMDD') as MEMB_CREA_DAY		--��������
		FROM t1_1 t1 
		LEFT JOIN (
				select PHMC_CODE,PHMC_TYPE,PROP_ATTR,ADMS_ORG_NAME,to_char(MOVE_DATE,'YYYY') as MOVE_YEAR	--Ǩַ���
				from DW.DIM_PHMC 
				where close_date is null
			) t2 
		on t1.PHMC_CODE=t2.PHMC_CODE
		INNER JOIN "DW"."FACT_MEMBER_BASE" t3
		on t1.MEMBER_ID=t3.MEMB_CODE
	)
	,
	--ͬ��ͬ����һ�λ�ͷ
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
	--ѡ��ھ�
	t2 as (
		select t1.MEMB_CODE as MEMBER_ID
			,to_char(t1.CREA_TIME,'YYYY') as MEMB_CREA_TEAR	--�������
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
	--�ȿ��׵�ת�������
	t3_1 as (
		select MEMBER_ID
			,MEMB_CREA_TEAR
			,buy_diff		--���Ѽ������
			,rn				--�ڼ�������
		from t2 
		where rn<=1
	
	)
	,
	t3_2 as (
		select MEMB_CREA_TEAR
			,buy_diff
			,count(1) as memb_num		--��Ա����
		from t3_1
		where buy_diff>=0
		group by MEMB_CREA_TEAR
			,buy_diff
	)
	,
	t3 as (
		select MEMB_CREA_TEAR
			,buy_diff
			,sum(memb_num) as memb_num --�ۻ����ѻ�Ա��
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
	--�ٿ�2���ֲ����
	t4_1 as (
		select MEMBER_ID
			,MEMB_CREA_TEAR
			,buy_diff		--���Ѽ������
			,rn				--�ڼ�������
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
	












