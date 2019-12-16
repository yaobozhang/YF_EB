--���⣺��ʧ�ߵȼ���Ա��л��
--BY�������ݹ��� Ҧ����
--DATE: 20191213

--STEP1���õ�Ŀ���Ա�����
--��Ա��ǩʱ��20191211
drop table "EXT_TMP"."YBZ_MEMB_INFO_20191213";
create column table "EXT_TMP"."YBZ_MEMB_INFO_20191213" as
(
        select member_id				--��ԱID
				,MEMB_LIFE_CYCLE		--��������
				,case when MAIN_CNSM_DEPRT_CODE='10011441' then 'changsha'													--�����ص�Ϊ��ɳ������
				else 'ningxiang' end as memb_place		--��Ա�����ѵط�
				,OFFLINE_LAST_CNSM_DATE	--���һ������ʱ��
				,MAIN_CNSM_PHMC_CODE	--�������ŵ�
		from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
		WHERE OFFLINE_LAST_CNSM_DATE>add_months('20191211',-18)		--���һ������ʱ����180������
		and MEMB_LIFE_CYCLE in ('06','07')																					--ѡ����������
		and (																												--ѡ���ص�
				MAIN_CNSM_DEPRT_CODE ='10011441' 	--��ɳ�Źܲ�
				OR 
				MAIN_CNSM_DIST_CODE IN ('10012924','10017063') --����һ�����������
			)
		and exists(																											--ѡ����������
			select 1 from (
				select MEMBER_ID
					,sum(SALE_AMT) as SALE_AMT
				from "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					'20191211'),	--��Աһ���������
					'PLACEHOLDER' = ('$$BeginTime$$',
					'20180611'))
				group by MEMBER_ID
			)t2 
			where t1.member_id=t2.member_id
			and SALE_AMT >= 400
		)
);
commit;

--STEP2:�õ���Ա�Ƽ���������Ʒ��Ϣ
--�ѻ�Ա�ؽ��з�װ������ͳһ�޸�Դ
with t1_1 as (
	select member_id
		,memb_life_cycle
		,memb_place
		,OFFLINE_LAST_CNSM_DATE			
		,MAIN_CNSM_PHMC_CODE
	from "EXT_TMP"."YBZ_MEMB_INFO_20191213"
	where memb_place='changsha'								--ѡ���ĸ��ط�����
)
,
--�õ���ͬ�ص����Ʒ������
t1_2 as (
	
	SELECT t1.DESEASE_LEV2_CODE		--��������
		,t1.GOODS_CODE				--������Ʒ
		,t1.GOODS_NAME				
		,case when t2.GOODS_CODE is not null then 1 else 0 end as changsha_flag		--�Ƿ�ɳ��Ʒ��
		,case when t2.GOODS_CODE is not null then 1 else 0 end as ningxiang_flag	--�Ƿ�������Ʒ��
	FROM 
	(
	SELECT DESEASE_LEV2_CODE,GOODS_CODE,max(GOODS_NAME) as GOODS_NAME		--���ȣ�ȥ�أ��õ�����������Ʒȥ�غ�����
	FROM "EXT_TMP"."BOZHANG_DISEASE_UNION_GOOD"
	GROUP BY DESEASE_LEV2_CODE,GOODS_CODE
	) t1
	left join "EXT_TMP"."BOZHANG_CHANGSHA_GOODS" t2		--������ɳ��Ʒ��
	on  T1.GOODS_CODE=T2.GOODS_CODE
	left join "EXT_TMP"."BOZHANG_NINGXIANG_GOODS" t3		--����������Ʒ��
	on T1.GOODS_CODE=T3.GOODS_CODE
)
,
t1_3 as (
	SELECT DESEASE_LEV2_CODE
		,GOODS_CODE
		,GOODS_NAME
	FROM T1_2
	WHERE changsha_flag=1								--ѡ���ĸ��ط�����Ʒ��

)
,
--�õ���Ʒ�������ݣ���������������
t1 as (
		SELECT
			 t."STSC_DATE",  								--��������
			 t."PHMC_CODE",     							--�ŵ����
			 t."MEMBER_ID",								--��Ա����
			 t."GOODS_CODE",    							--��Ʒ����
			g.PROD_CATE_LEV1_CODE,
			g.PROD_CATE_LEV4_CODE,
			g.PROD_CATE_LEV4_NAME,
			g.GOODS_NAME,			--��Ʒ����
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--ë����
			 sum("SALE_AMT") AS "SALE_AMT",				--���۶�
			 sum("SALE_QTY") AS "SALE_QTY",				--��������
			MAX(ACNT_PRIC) AS ACNT_PRIC		--��Ʒ����
			,t1.memb_place
			,t1.OFFLINE_LAST_CNSM_DATE			
			,t1.MAIN_CNSM_PHMC_CODE
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191211'),				--��ǰ��Ա��ǩ
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180611')) t 			--��ǰһ���
		inner join t1_1 t1
		on t.MEMBER_ID=t1.MEMBER_ID
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid		--ʹ��left join����ֹ��Ʒ������
		GROUP BY t."STSC_DATE"                                  --��������
			,t."PHMC_CODE"                                  --�ŵ����
			,t."MEMBER_ID"									 --��Ա����
			,t."GOODS_CODE"   							--��Ʒ����
			,g.PROD_CATE_LEV4_CODE
			,g.PROD_CATE_LEV4_NAME
			,g.GOODS_NAME			--��Ʒ����
			,g.PROD_CATE_LEV1_CODE
			,t1.memb_place
			,t1.OFFLINE_LAST_CNSM_DATE			
			,t1.MAIN_CNSM_PHMC_CODE

)

,
--��ÿ���ط�������������ǰ300��Ʒ
t2 as (
	select memb_place,GOODS_CODE,GOODS_NAME,memb_num
		,row_number() over(partition by memb_place order by memb_num desc) as rn
	from
	(
		select memb_place,GOODS_CODE,GOODS_NAME,count(distinct MEMBER_ID) as memb_num
		from t1
		where GOODS_NAME is not null
		AND PROD_CATE_LEV1_CODE!='Y11'	--��Ҫ���˻���
		AND  PROD_CATE_LEV1_CODE!='Y13'		--��Ҫ�ճ���Ʒ
		AND PROD_CATE_LEV4_NAME!='ɢװ��'
		group by memb_place,GOODS_CODE,GOODS_NAME
	)

)
,
--��ÿ���ط��˾�������������ǰ300��Ʒ
t3 as (
	select memb_place,GOODS_CODE,GOODS_NAME,memb_avg_num,memb_num
		,row_number() over(partition by memb_place order by memb_avg_num desc) as rn
	from
	(
		select memb_place,GOODS_CODE,GOODS_NAME,sum(SALE_QTY)/count(distinct MEMBER_ID) as memb_avg_num,count(distinct MEMBER_ID) as memb_num
		from t1
		where GOODS_NAME is not null
		AND PROD_CATE_LEV1_CODE!='Y11'
		AND  PROD_CATE_LEV1_CODE!='Y13'
		AND PROD_CATE_LEV4_NAME!='ɢװ��'
		group by memb_place,GOODS_CODE,GOODS_NAME
	)

)
,
--STEP4:�õ���Ա������ǩ
--���ȣ�ͬ��ͬ����һ��
t4_1 as (
	SELECT STSC_DATE,
		GOODS_CODE,
		MEMBER_ID,
		max(memb_place) as memb_place,		--��Ա�ص�
		SUM(SALE_AMT) AS SALE_AMT,	--���۶�
		SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
	FROM t1
	GROUP BY STSC_DATE,
		GOODS_CODE,
		MEMBER_ID
	
) 
--�õ�����-ҩ����
--���ȵõ�������ҩ�����õ�ÿ��ҩ������
,
t4_2 as (
	select GOODS_CODE,count(1) as num from "DS_COL"."SUCCEZ_GOODS_DISEASE" where EFFECT_SCORE=2 group by GOODS_CODE
)
--Ȼ��ȡ����������ֻ��һ��������ҩ�ĵ�Ʒ
,
t4_3 as (
	select t0.GOODS_CODE,t1.DISEASE_LV1_NAME DISEASE_NAME_LEV1
		,t1.DISEASE_LV2_NAME DISEASE_NAME_LEV2 
		,t1.DISEASE_LV2_CODE DISEASE_CODE_LEV2
	from
	(
		select GOODS_CODE from t4_2 where num=1
	)t0
	left join 
	(
		SELECT * FROM "DS_COL"."SUCCEZ_GOODS_DISEASE" where EFFECT_SCORE=2
	)
	t1 
	on t0.GOODS_CODE=t1.GOODS_CODE
) 
,
--�����õ�ÿ����Աÿ������ļ�������������˵�����Ϊ�յ�����
t4_4 as (
	select  t1.MEMBER_ID,						--��Ա����
		 t1.STSC_DATE,  					--��������
		 t1.GOODS_CODE,    				--��Ʒ����
		 t1.memb_place,
		 t2.DISEASE_NAME_LEV1,			--����һ��
		 t2.DISEASE_NAME_LEV2			--��������
		,t2.DISEASE_CODE_LEV2			--��������	
	from t4_1 t1 
	left join t4_3 t2
	on t1.GOODS_CODE=t2.GOODS_CODE

)
,
t4_5 as (
	select MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
		,max(memb_place) as memb_place
	FROM t4_4
	where DISEASE_NAME_LEV2 is not null
	group by MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
)

--step4:��ÿ����Ա����ȡ��������������༲��
,
t4 as (
	select member_id,memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,rn
	from
	(
		select t1.member_id,t2.memb_place,T2.DISEASE_CODE_LEV2,t2.DISEASE_NAME_LEV2,t2.day_num,row_number() over(partition by T1.member_id order by day_num desc) as rn
		from
		(
			select distinct member_id
			from t4_4
		)t1
		left join 
		(
			select member_id,max(memb_place) as memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,count(1) as day_num 
			from t4_5
			group by member_id,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2
		)t2
		ON T1.member_id=T2.member_id
		
	)
	where rn=1
)
,
--�õ���Ա����������Ʒ����Ҫѡ�����
t5_1 as (
	SELECT T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,CASE WHEN T2.GOODS_CODE IS NOT NULL THEN 1 ELSE 0 END AS IF_BUF_FLAG
		,T2.LAST_BUY_DATE		--���һ�ι���ʱ��
		,T2.MAX_PRICE			--��߳ɽ���
		,T2.MIN_PRICE			--��ͳɽ���
	FROM
	(
		SELECT T1.member_id			--��ԱID
			,memb_place				--��Ա����������
			,T1.DISEASE_CODE_LEV2	--��Ա����
			,T1.DISEASE_NAME_LEV2
			,T2.GOODS_CODE			--��Ա����������Ʒ
			,T2.GOODS_NAME
		FROM T4 T1
		LEFT JOIN t1_3 T2
		ON T1.DISEASE_CODE_LEV2=T2.DESEASE_LEV2_CODE
	)T1
	LEFT JOIN 
	(
		SELECT MEMBER_ID
			,GOODS_CODE
			,MAX(STSC_DATE) AS LAST_BUY_DATE		--���һ�ι���ʱ��
			,MAX(ACNT_PRIC) AS MAX_PRICE			--��߳ɽ���
			,MIN(ACNT_PRIC) AS MIN_PRICE			--��ͳɽ���
		FROM T1 
		GROUP BY MEMBER_ID,GOODS_CODE
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID 
	AND T1.GOODS_CODE=T2.GOODS_CODE
)
,
--�ҵ�ÿ����Աͨ�����������Ƽ�����Ʒ
T5 AS (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,t1.IF_BUF_FLAG
		,T1.LAST_BUY_DATE		--���һ�ι���ʱ��
		,T1.MAX_PRICE			--��߳ɽ���
		,T1.MIN_PRICE			--��ͳɽ���
	from
	(
		SELECT T1.member_id
			,T1.DISEASE_CODE_LEV2
			,T1.DISEASE_NAME_LEV2
			,T1.GOODS_CODE
			,T1.GOODS_NAME
			,t1.IF_BUF_FLAG
			,T1.LAST_BUY_DATE		--���һ�ι���ʱ��
			,T1.MAX_PRICE			--��߳ɽ���
			,T1.MIN_PRICE			--��ͳɽ���
			,row_number() over(partition by member_id order by IF_BUF_FLAG desc,GOODS_CODE asc) as rn
		FROM T5_1 t1
	)t1
	where rn=1
)
,

--�õ�ÿ����Ա������Ϣ
t6 as (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,t1.IF_BUF_FLAG
		,T1.LAST_BUY_DATE		--���һ�ι���ʱ��
		,T1.MAX_PRICE			--��߳ɽ���
		,T1.MIN_PRICE			--��ͳɽ���
		,T2.MEMB_NAME			--����
		,T2.MEMB_GNDR			--�Ա�
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--����
		,T2.MEMB_MOBI				--�ֻ���
		,T2.BELONG_PHMC_CODE		--�����ŵ�
		,t3.memb_place				--����������
		,t3.OFFLINE_LAST_CNSM_DATE			--���һ������ʱ��
		,t3.MAIN_CNSM_PHMC_CODE			--�������ŵ�
		,t4.sale_amt
		,t4.sale_times
		,T2.MEMB_CARD_CODE
	from t5 T1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
	left join t1_1 t3
	on t1.member_id=t3.member_id
	left join (
		select member_id
			,sum(sale_amt) as sale_amt
			,count(1) as sale_times
		from t1
		group by member_id
	)t4
	on t1.member_id=t4.member_id
)
,
--
t7_1 as (
	select MEMBER_ID
			,GOODS_CODE
			,GOODS_NAME
			,LAST_BUY_DATE		--���һ�ι���ʱ��
			,MAX_PRICE			--��߳ɽ���
			,MIN_PRICE			--��ͳɽ���
		from
		(
			select MEMBER_ID
				,GOODS_CODE
				,GOODS_NAME
				,LAST_BUY_DATE		--���һ�ι���ʱ��
				,MAX_PRICE			--��߳ɽ���
				,MIN_PRICE			--��ͳɽ���
				,row_number() over(partition by member_id order by SALE_QTY desc) as rn
			from
			(
				SELECT MEMBER_ID
					,GOODS_CODE
					,max(GOODS_NAME) as GOODS_NAME
					,MAX(STSC_DATE) AS LAST_BUY_DATE		--���һ�ι���ʱ��
					,sum(SALE_QTY) as SALE_QTY		--��������
					,MAX(ACNT_PRIC) AS MAX_PRICE			--��߳ɽ���
					,MIN(ACNT_PRIC) AS MIN_PRICE			--��ͳɽ���
				FROM T1
				where exists(
					select 1 from
					t1_3 t2
					where t1.goods_code=t2.goods_code
				)
				group by member_id,goods_code
			)
		)where rn=1
)
,
--���ûƥ�䵽ҩƷ����ÿ���˻��Ʒ�����������Ϊ����
t7 as (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,case when T1.GOODS_CODE is not null then T1.GOODS_CODE else t2.GOODS_CODE end as GOODS_CODE
		,case when T1.GOODS_CODE is not null then T1.GOODS_NAME else t2.GOODS_NAME end as GOODS_NAME
		,t1.IF_BUF_FLAG
		,case when T1.GOODS_CODE is not null then T1.LAST_BUY_DATE else T2.LAST_BUY_DATE end as LAST_BUY_DATE		--���һ�ι���ʱ��
		,case when T1.GOODS_CODE is not null then T1.MAX_PRICE else T2.MAX_PRICE end as MAX_PRICE 			--��߳ɽ���
		,case when T1.GOODS_CODE is not null then T1.MIN_PRICE else T2.MIN_PRICE end as MIN_PRICE 			--��ͳɽ���
		,T1.MEMB_NAME			--����
		,T1.MEMB_GNDR			--�Ա�
		,t1.AGE 	--����
		,T1.MEMB_MOBI				--�ֻ���
		,T1.BELONG_PHMC_CODE		--�����ŵ�
		,t1.memb_place				--����������
		,t1.OFFLINE_LAST_CNSM_DATE			--���һ������ʱ��
		,t1.MAIN_CNSM_PHMC_CODE			--�������ŵ�
		,t1.sale_amt
		,t1.sale_times
		,T1.MEMB_CARD_CODE
	from t6 t1
	left join t7_1 t2
	on t1.member_id=t2.member_id
)
select * from t7














