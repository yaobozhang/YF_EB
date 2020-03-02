--Ŀ�ģ���������ʱΪ�����Ͼ��ߵȼ���Ա����ԤԼ 
--���빱���ߣ�Ҧ����
--ʱ�䣺20200228


--��ϸ����1���߶˻�Ա��лԤԼ
--1.1���������ŵ��Ӧ����Ϊ�����Ͼ�
--1.2���������Ա����>=600
--1.3����180��������  OK
--1.4�����˵���30�칺������� 
--1.5�����˵���55�쵥�ι�����>600��
--1.6�����վ���ֵ�Ӵ�С����
--2���ϴ��»
--2.1���������ŵ��Ӧ����Ϊ�����Ͼ�
--2.2����������Ϊ�ҳϺͻ�Ծ
--2.3�����˵���30�칺�������
--2.4�����վ���ֵ�Ӵ�С����


--STEP1���߶˻�Ա��лԤԼ
--1.1���������ŵ��Ӧ����Ϊ�����Ͼ�
with t0 as (
	select member_id
		,SALE_AMT
		,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--�Ƿ�ߵȼ�
		,kouzhao_flag --��30���������
		,day_55_max	--��55�쵥�ι�����
	from
	(
		SELECT
			t."MEMBER_ID"
			,sum(SALE_AMT) as SALE_AMT
			,max(case when g.GOODS_NAME like '%����%' and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --��30���������
			,max(CASE WHEN stsc_date>=ADD_DAYS('20200228',-55) THEN SALE_AMT END) as day_55_max	--��55�쵥�ι�����
		FROM dw.fact_sale_ordr_detl t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		and stsc_date>='20180228' and stsc_date<'20200228'
		GROUP BY
			 t."MEMBER_ID"									 --��Ա����
	)
)
,
t1 as (
	 select t1.member_id				--��ԱID
		,t1.MEMB_LIFE_CYCLE		--��������
		,t1.OFFLINE_LAST_CNSM_DATE	--���һ������ʱ��
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,floor(t3.SALE_AMT) as 	SALE_AMT	--�ɳ�ֵ
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--�������ŵ�Ϊ�����Ͼ�
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%�Ͼ�%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--������Աɸѡ����
		(
			select member_id
				,SALE_AMT
			from t0 t2
			where t2.high_level_memb=1
			and t2.kouzhao_flag=0
			and t2.day_55_max<=600
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--���һ������ʱ����180������

)
select count(1) from t1


--STEP2���ϴ��»
with t0 as (
	select member_id
		,SALE_AMT
		,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--�Ƿ�ߵȼ�
		,kouzhao_flag --��30���������
	from
	(
		SELECT
			t."MEMBER_ID"
			,sum(SALE_AMT) as SALE_AMT
			,max(case when g.GOODS_NAME like '%����%' and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --��30���������
		FROM dw.fact_sale_ordr_detl t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		and stsc_date>='20200120' and stsc_date<'20200228'
		GROUP BY
			 t."MEMBER_ID"									 --��Ա����
	)
)
,
t1 as (
	 select t1.member_id				--��ԱID
		,t1.MEMB_LIFE_CYCLE		--��������
		,t1.OFFLINE_LAST_CNSM_DATE	--���һ������ʱ��
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,floor(t3.SALE_AMT) as 	SALE_AMT	--�ɳ�ֵ
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--�������ŵ�Ϊ�����Ͼ�
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%�Ͼ�%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--������Աɸѡ����
		(
			select member_id
				,SALE_AMT
			from t0 t2
			where t2.kouzhao_flag=0
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--���һ������ʱ����180������
	and MEMB_LIFE_CYCLE in ('03','04')		

)
select count(1) from t1




----------------------------------------------------------------------------------------------------
--�ܲ��������
--������������������궩���õ��ɳ�ֵ��������������
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" as
(
		select member_id
					,SALE_AMT
					,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--�Ƿ�ߵȼ�
					,kouzhao_flag --��30���������
					,day_55_max	--��55�쵥�ι�����
				from
				(
					SELECT
						t."MEMBER_ID"
						,sum(SALE_AMT) as SALE_AMT
						,max(case when g.GOODS_NAME IS NOT NULL and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --��30���������
						,max(CASE WHEN stsc_date>=ADD_DAYS('20200228',-55) THEN SALE_AMT END) as day_55_max	--��55�쵥�ι�����
					FROM dw.fact_sale_ordr_detl t 
					left join (
						select GOODS_CODE,GOODS_NAME
						FROM
						dw.DIM_GOODS_cong
						WHERE GOODS_NAME like '%����%'
						) g on g.goods_CODE=t.goods_CODE
					where t.member_id is not null
					and stsc_date>='20180228' and stsc_date<'20200228'
					GROUP BY
						 t."MEMBER_ID"									 --��Ա����
				)
);
commit;


--Ȼ�󣬵õ��߶˻�Ա����ԤԼ����
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228" as
(
	select t1.member_id				--��ԱID
		,t1.MEMB_LIFE_CYCLE		--��������
		,t1.OFFLINE_LAST_CNSM_DATE	--���һ������ʱ��
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,floor(t3.SALE_AMT) as 	SALE_AMT	--�ɳ�ֵ
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--�������ŵ�Ϊ�����Ͼ�
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%�Ͼ�%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--������Աɸѡ����
		(
			select member_id
				,SALE_AMT
			from "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" t2
			where t2.high_level_memb=1
			and t2.kouzhao_flag=0
			and t2.day_55_max<=600
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--���һ������ʱ����180������

)

)

--�õ���Ծ��Ա����
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228_act" as
(
	select t1.member_id				--��ԱID
		,t1.MEMB_LIFE_CYCLE		--��������
		,t1.OFFLINE_LAST_CNSM_DATE	--���һ������ʱ��
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,floor(t3.SALE_AMT) as 	SALE_AMT	--�ɳ�ֵ
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--�������ŵ�Ϊ�����Ͼ�
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%�Ͼ�%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--������Աɸѡ����
		(
			select member_id
				,SALE_AMT
			from "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" t2
			where t2.kouzhao_flag=0
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--���һ������ʱ����180������
	and MEMB_LIFE_CYCLE in ('03','04')		
	
)




--�ߵȼ���Ա�����õ���Ա���ţ��������Ա����䣬��Ա�ֻ�
create column table "EXT_TMP"."YBZ_20200228_high" as
(
	select t1.member_id
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,t1.SALE_AMT	--�ɳ�ֵ
		,t2.memb_card_code
		,T2.MEMB_NAME			--����
		,T2.MEMB_GNDR			--�Ա�
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--����
		,T2.MEMB_MOBI				--�ֻ���
	from "EXT_TMP"."YBZ_MEMB_INFO_20200228" t1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
)

--��Ծ��Ա�����õ���Ա���ţ��������Ա����䣬��Ա�ֻ�
create column table "EXT_TMP"."YBZ_20200228_act" as
(
	select t1.member_id
		,t1.MAIN_CNSM_PHMC_CODE	--�������ŵ�
		,t1.MAIN_CNSM_PHMC_NAME	--�������ŵ�����
		,t1.SALE_AMT	--�ɳ�ֵ
		,t2.memb_card_code
		,T2.MEMB_NAME			--����
		,T2.MEMB_GNDR			--�Ա�
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--����
		,T2.MEMB_MOBI				--�ֻ���
	from "EXT_TMP"."YBZ_MEMB_INFO_20200228_act" t1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
)