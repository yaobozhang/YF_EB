--����ʶ������
--���빱���ߣ�Ҧ����
--�������ʱ�䣺20191018
--���ݿھ���������ģ��

--����ʶ����Ҫ�����¼���
--step1:���ȣ��õ�һ�궩�����������Ʒ���кϲ�,����Ա���ŵꡢ��Ʒ����
	with t1_1 as (
			SELECT
				 t."STSC_DATE",  								--��������
				 t."GOODS_CODE",    							--��Ʒ����
				 t."PHMC_CODE",
				 t."MEMBER_ID",								--��Ա����
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--ë����
				 sum("SALE_AMT") AS "SALE_AMT"				--���۶�
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191031'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20181031')) t 
			where member_id is not null
			and not exists(
				select 1 from "DW"."DIM_PHMC" t1
				where t.PHMC_CODE=t1.PHMC_CODE
				and t1.ADMS_ORG_CODE='1025'	--���˵��ӱ�����
			)
			GROUP BY t."STSC_DATE",                                  --��������
				 t."GOODS_CODE",                                 --��Ʒ����
				 t."PHMC_CODE",
				 t."MEMBER_ID"									 --��Ա����
		)
		,
		t1 as (
			SELECT STSC_DATE,
				GOODS_CODE,
				MEMBER_ID,
				SUM(SALE_AMT) AS SALE_AMT,	--���۶�
				SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
			FROM t1_1
			GROUP BY STSC_DATE,
				GOODS_CODE,
				MEMBER_ID
			
		)        

/*
"EXT_TMP"."BOZHANG_MEDI_DISEASE" ("GOODS_CODE" NVARCHAR(10),
	 "GOODS_NAME" NVARCHAR(100),
	 "DISEASE_CODE_LEV1" NVARCHAR(3),
	 "DISEASE_NAME_LEV1" NVARCHAR(20),
	 "DISEASE_CODE_LEV2" NVARCHAR(5),
	 "DISEASE_NAME_LEV2" NVARCHAR(20),
	 "POINT" INT) UNLOAD PRIORITY 5 AUTO MERGE 

*/
--step2:�õ�����-ҩ����
--���ȵõ�������ҩ�����õ�ÿ��ҩ������
	,
	t2_0 as (
		select GOODS_CODE,count(1) as num from "EXT_TMP"."BOZHANG_MEDI_DISEASE" where POINT=2 group by GOODS_CODE
	)
--step2.1 Ȼ��ȡ����������ֻ��һ��������ҩ�ĵ�Ʒ
	,
	t2_1 as (
		select t0.GOODS_CODE,t1.DISEASE_NAME_LEV1,t1.DISEASE_NAME_LEV2 from
		(
			select GOODS_CODE from t2_0 where num=1
		)t0
		left join 
		(
			SELECT * FROM "EXT_TMP"."BOZHANG_MEDI_DISEASE" where POINT=2
		)
		t1 
		on t0.GOODS_CODE=t1.GOODS_CODE
	)
	
--step2.2 ��Ȼ��ȡ����������������������������ҩ�ĵ�Ʒ
	


--step3.1:��step1�е�������step2.1�����õ�ÿ����Աÿ������ļ�������������˵�����Ϊ�յ�����
	,
	t3_1_1 as (
		select  t1.MEMBER_ID,						--��Ա����
			 t1.STSC_DATE,  					--��������
			 t1.GOODS_CODE,    				--��Ʒ����
			 t2.DISEASE_NAME_LEV1,			--����һ��
			 t2.DISEASE_NAME_LEV2			--��������
		from t1 
		left join t2_1 t2
		on t1.GOODS_CODE=t2.GOODS_CODE
	
	)
	,
	t3_1 as (
		select MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
		FROM t3_1_1
		WHERE  DISEASE_NAME_LEV2 is not null 
		group by MEMBER_ID
			,STSC_DATE
			,DISEASE_NAME_LEV2
	)
	

--step3.2:��step1�е�������step2.2�е����ݽ��й�����ÿ����Աÿ��������ɼ������ݣ����ռ������ͼ��������������ȡ��һ�����������˵�����Ϊ�յ�����


--step4:��ÿ����Ա���ݰ��ռ������л��ܣ���Ȼ��Ѹñ�����Ϊ��ǩ������ͼ��ǩ��ʵ�֣����Խ������ʵ�ַ�ʽ��
	,
	t4 as (
		select member_id,DISEASE_NAME_LEV2,count(1) as day_num 
		from t3_1
		group by member_id,DISEASE_NAME_LEV2
	)
	
--step5:ͳ��ÿ�������л�Ա���������
	,
	t5 as(
		SELECT t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.GROS_PROF_AMT
			,t1.NUM as sale_times
			,t2.MEMB_LIFE_CYCLE
			,t4.DISEASE_NAME_LEV2
		FROM
		(
			select member_id
				,SUM(SALE_AMT) AS SALE_AMT
				,SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
				,COUNT(1) AS NUM
			from
			(
				select STSC_DATE,
					PHMC_CODE,
					MEMBER_ID,
					SUM(SALE_AMT) AS SALE_AMT,	--���۶�
					SUM(GROS_PROF_AMT) AS GROS_PROF_AMT --ë����
				FROM t1_1 
				GROUP BY MEMBER_ID,STSC_DATE,PHMC_CODE
			)
			group by member_id
		) t1 
		LEFT JOIN 
		(
			SELECT MEMBER_ID,MEMB_LIFE_CYCLE 
			FROM DM.FACT_MEMBER_CNT_INFO 
			WHERE DATA_DATE='20191031'
		)t2
		ON t1.member_id=t2.member_id
		LEFT JOIN t4 
		on t1.MEMBER_ID=t4.member_id
	)
	,
--STEP6:ͳ��ÿ���������������������
	t6 as (
		select t1.member_id
			,t1.DISEASE_NAME_LEV2 as NAME1
			,t2.DISEASE_NAME_LEV2 as NAME2
		from t4 t1
		left join t4 t2
		on t1.member_id=t2.member_id
		and t1.DISEASE_NAME_LEV2<t2.DISEASE_NAME_LEV2
	)
	,
	t7 as (
		SELECT t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.GROS_PROF_AMT
			,t1.NUM as sale_times
			,t6.NAME1
			,t6.NAME2
		FROM
		(
			select member_id
				,SUM(SALE_AMT) AS SALE_AMT
				,SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
				,COUNT(1) AS NUM
			from
			(
				select STSC_DATE,
					PHMC_CODE,
					MEMBER_ID,
					SUM(SALE_AMT) AS SALE_AMT,	--���۶�
					SUM(GROS_PROF_AMT) AS GROS_PROF_AMT --ë����
				FROM t1_1 
				GROUP BY MEMBER_ID,STSC_DATE,PHMC_CODE
			)
			group by member_id
		) t1 
		LEFT JOIN t6 
		on t1.MEMBER_ID=t6.member_id
	
	)
	select NAME1
		,NAME2
		,count(distinct member_id) as memb_num --��Ա��
		,avg(SALE_AMT) as memb_year_sale
		,avg(GROS_PROF_AMT) as memb_year_gros
		,avg(sale_times) as memb_year_times
	from t7
	group by NAME1
		,NAME2
	
	select DISEASE_NAME_LEV2
		,MEMB_LIFE_CYCLE
		,count(distinct member_id) as memb_num --��Ա��
		,avg(SALE_AMT) as memb_year_sale
		,avg(GROS_PROF_AMT) as memb_year_gros
		,avg(sale_times) as memb_year_times
	from t5
	group by DISEASE_NAME_LEV2
		,MEMB_LIFE_CYCLE

		

--������ͼ�����������ڣ���������










