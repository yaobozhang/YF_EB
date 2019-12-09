	with t1 as (
			SELECT
				 t."UUID",	   								--��ϸΨһ����
				 t."STSC_DATE",  								--��������
				 t."SALE_ORDR_DOC",  							--���۶�����
				 t."PHMC_CODE",     							--�ŵ����
				 t."GOODS_CODE",    							--��Ʒ����
				 t."MEMBER_ID",								--��Ա����
				 g.PROD_CATE_LEV1_CODE,						--Ʒ�����ר�ã�����ʱע��
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--���
				 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--�·�
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --���ջ�Ա���루�ǻ�Ա�Զ�����Ϊ���룩
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --�Ƿ��ǻ�Ա
				 sum( case when g.PURC_CLAS_LEV1_CODE='01' then t.sale_amt end) as PURC_MONEY, 			--Ӫ������
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--ë����
				 sum(t."GOODS_SID") AS "GOODS_SID",			--��Ʒ���������Ʒ��Ψһ����
				 sum("SALE_QTY") AS "SALE_QTY",				--��������
				 sum("SALE_AMT") AS "SALE_AMT"				--���۶�
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20150101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			GROUP BY t."UUID",								   --��ϸΨһ����											   
				 t."STSC_DATE",                                  --��������
				 t."SALE_ORDR_DOC",                              --���۶�����
				 t."PHMC_CODE",                                  --�ŵ����
				 t."GOODS_CODE",                                 --��Ʒ����
				 t."MEMBER_ID",									 --��Ա����
				 g.PROD_CATE_LEV1_CODE,						--Ʒ�����ר�ã�����ʱע��
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME
		)                    
		--2.3.1��ȡ�������ݵĿھ�����t1����ͬ��ͬ��ͬ�ŵ���һ�δ�������ͳ��ָ�꣬�õ�Ӫ�����۶����Ʒ��
	,t2 as (
		select
			stsc_date,					--����
			member_id,					--��ԱID
			phmc_code,					--�ŵ��
			AT_TEAR,					--��ݴ�����
			sum(sale_amt) as sale_amt, 	--���۶�
			sum(PURC_MONEY) as PURC_MONEY,--Ӫ�����۶�
			sum(GROS_PROF_AMT) as gros	--ë����
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			phmc_code,
			AT_TEAR
	)
	,
	--Ʒ��ר��
	t2_1 as (
		--�õ�ƴ�Ӻ��Ʒ��
		select member_id
			,AT_TEAR
			,PROD_CATE_LEV2_NAME
			,sum(SALE_AMT) as SALE_AMT
			,count(1) as sale_times
		from
		(
			select	stsc_date,					--����
				member_id,					--��ԱID
				phmc_code,					--�ŵ��
				AT_TEAR,					--��ݴ�����
				PROD_CATE_LEV2_NAME,
				sum(SALE_AMT) as SALE_AMT
			from
			(
				select stsc_date,					--����
					member_id,					--��ԱID
					phmc_code,					--�ŵ��
					AT_TEAR,					--��ݴ�����
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'����ҩ'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'�Ǵ���ҩ'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'����') END PROD_CATE_LEV2_NAME
				,SALE_AMT
				from t1 s
				left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
				on S.GOODS_CODE = T.GOODS_CODE
			)
			group by stsc_date,					--����
				member_id,					--��ԱID
				phmc_code,					--�ŵ��
				AT_TEAR,					--��ݴ�����
				PROD_CATE_LEV2_NAME
		)
		group by member_id
			,AT_TEAR
			,PROD_CATE_LEV2_NAME
	)
	,
	--����ר��
	t2_2 as (
		select
			stsc_date,					--����
			member_id,					--��ԱID
			phmc_code,					--�ŵ��
			AT_TEAR,					--��ݴ�����
			AT_MONTH,					--�·ݴ�����
			sum(sale_amt) as sale_amt, 	--���۶�
			sum(PURC_MONEY) as PURC_MONEY,--Ӫ�����۶�
			sum(GROS_PROF_AMT) as gros	--ë����
		from t1
		where is_member='Y'
		group by 
			stsc_date,
			member_id,
			phmc_code,
			AT_TEAR,
			AT_MONTH
	)
	,
	--ÿ��ÿ�����ͳ��
	t3_0 as
	(
		select 
				 AT_TEAR,
				 MEMBER_ID,
				 sum(SALE_AMT) as SALE_AMT,
				 count(1) as sale_times,
				 sum(PURC_MONEY) as PURC_MONEY--Ӫ�����۶�
			 from t2 
			 group by AT_TEAR,
				MEMBER_ID
	)
	,
	--�õ���Ա�����ŵ��Ƿ��չ����ˡ��Ƿ����ϵꡢ���ĸ��ֹ�˾
	--���ȣ��õ���Ա�����ŵ�
	t3_1 as
	(
		select t1.MEMBER_ID			--��Ա����
			,t1.AT_STORE			--�����ŵ�
			,t2.ADMS_ORG_CODE		--�ֹ�˾����
			,t2.ADMS_ORG_NAME		--�ֹ�˾����
			,t2.company_code		--��˾����='4000'
			,t2.PROP_ATTR			--�����ֶ� in ('Z02','Z07')
		from
		(
			select t1.MEMBER_ID			--��Ա����
				,t2.AT_STORE			--�����ŵ�
			from
			(
				select 
					MEMBER_ID	--��Ա����
				from t3_0
				group by member_id
			)t1
			left join  
			(
				select 
					customer_id,
					AT_STORE
				from ds_crm.tp_cu_customerbase
			)t2
			on t1.member_id=t2.customer_id
		)t1
		left join dw.DIM_PHMC t2
		on t1.AT_STORE=t2.PHMC_CODE	
	)
	,
	--�ж������ŵ�����
	t3 as
	(
		select  t1.AT_TEAR
			,t1.MEMBER_ID
			,t1.SALE_AMT
			,t1.PURC_MONEY	--Ӫ�����۶�
			,t1.sale_times
			,case when left(t2.company_code,1)=4 or t2.PROP_ATTR in ('Z02','Z07') then 'SG_JM' else 'NORMAL' end as SG_JM_FLAG		--�Ƿ��չ�����
			,t2.ADMS_ORG_NAME		--�ֹ�˾����
		from t3_0 t1 
		left join t3_1 t2
		on t1.MEMBER_ID=t2.member_id
	)
	,
	--����ר�ã������õ���Ա��˾���Ƿ��չ�����
	t3_2_1 as
	(
		select t1.member_id,				--��ԱID
			t1.AT_TEAR,					--��ݴ�����
			t1.AT_MONTH,					--�·ݴ�����
			t1.sale_amt, 					--���۶�
			t1.PURC_MONEY,					--Ӫ�����۶�
			t1.gros,						--ë����
			t1.sale_times,
			t2.SG_JM_FLAG,				--�Ƿ��չ�����
			t2.ADMS_ORG_NAME			--�ֹ�˾����
		from
		(
			select
				member_id,					--��ԱID
				AT_TEAR,					--��ݴ�����
				AT_MONTH,					--�·ݴ�����
				sum(sale_amt) as sale_amt, 	--���۶�
				count(1) as sale_times,		--���۴���
				sum(PURC_MONEY) as PURC_MONEY,--Ӫ�����۶�
				sum(GROS_PROF_AMT) as gros	--ë����
			from t1
			group by 
				member_id,
				AT_TEAR,
				AT_MONTH
		)t1
		left join
		(
			select MEMBER_ID
				,max(SG_JM_FLAG) as SG_JM_FLAG		--�Ƿ��չ�����
				,max(ADMS_ORG_NAME) as ADMS_ORG_NAME		--�ֹ�˾����
			from t3
			group by member_id
		)t2
		on t1.member_id=t2.member_id
	)
	,
	--����ר�ã����������ݣ��õ�ÿ���µĸ�����ʶ
	t3_2 as
	(
		select t1.member_id,				--��ԱID
			t1.AT_TEAR,						--��ݴ�����
			t1.AT_MONTH,					--�·ݴ�����
			t1.sale_amt, 					--���۶�
			t1.PURC_MONEY,					--Ӫ�����۶�
			t1.sale_times,			
			t1.gros,							--ë����
			t1.SG_JM_FLAG,					--�Ƿ��չ�����
			t1.ADMS_ORG_NAME,				--�ֹ�˾����
			case when t2.member_id is not null then 1 else 0 end as IS_CB_FLAG		--�Ƿ񸴹�
		from t3_2_1 t1
		left join t3_0 t2 
		on t1.member_id=t2.member_id
		and t1.AT_TEAR=t2.AT_TEAR+1
		where t1.SG_JM_FLAG='NORMAL'		--ֻ�������չ����˵�
	)
	,
	--ȡ����ȸ�����Ա�������۶���Ѵ���
	t4 as (
		select	AT_TEAR				--���
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--��������
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--�������
			,sum(case when member_id is not null then PURC_MONEY else 0 end) as return_memb_sale_purc	--����Ӫ�����
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--�������
		from
		(
			select  
				 t1.AT_TEAR,				--���
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--�ֹ�˾����
				 t2.member_id ,				--��һ���Ƿ���
				 t1.SALE_AMT	,			--���۶�
				 t1.sale_times				--���Ѵ���
				 ,t1.PURC_MONEY	--Ӫ�����۶�
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
		)
		group by AT_TEAR
	)
	,
	--�����������ѻ�Ա����	  
	t5 as ( 
		select 
			AT_TEAR,
			count(1) as total_qty --���ѻ�Ա����	
		from t3
		group by AT_TEAR
	 )
	 ,
	 --�õ�����ȸ�����Ա�������ѻ�Ա���������˾����Ѷ�����˾�����Ƶ��
	 t6 as 
	 (
		select 	 
			t5.AT_TEAR,					--���
			t5.total_qty,				--����������
			t4.return_memb_num,			--��������	
			t4.return_memb_sale, 		--�����۶�
			t4.return_memb_times, 		--�����Ѵ���
			t4.return_memb_sale_purc	--����Ӫ�����
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale/t4.return_memb_num end as return_memb_avg_sale		--�˾����۽��
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_times/t4.return_memb_num end as return_memb_avg_times	--�˾����۴���
			,case when t4.return_memb_num=0 then 0 else t4.return_memb_sale_purc/t4.return_memb_num end as return_memb_avg_purc	--�˾�����Ӫ�����
		from t5
		left join t4
		on t5.AT_TEAR=t4.AT_TEAR
	 )
	,
	--ȡ������չ����˸�����Ա�������۶���Ѵ���
	t4_1 as (
		select	AT_TEAR				--���
			,SG_JM_FLAG
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--��������
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--�������
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--�������
		from
		(
			select  
				 t1.AT_TEAR,				--���
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--�ֹ�˾����
				 t2.member_id ,				--��һ���Ƿ���
				 t1.SALE_AMT	,			--���۶�
				 t1.sale_times				--���Ѵ���
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
		)
		group by AT_TEAR
		,SG_JM_FLAG
	)
	,
	--�����������ѻ�Ա����	  
	t5_1 as ( 
		select 
			AT_TEAR,
			SG_JM_FLAG,
			count(1) as total_qty --���ѻ�Ա����	
		from t3
		group by AT_TEAR
		,SG_JM_FLAG
	 )
	 ,
	 --�õ�������Ƿ��չ����˵ĸ�����Ա�������ѻ�Ա���������˾����Ѷ�����˾�����Ƶ��
	 t6_1 as 
	 (
		select 	 
			t5.AT_TEAR,					--���
			t5.SG_JM_FLAG,				--�Ƿ��չ�����
			t5.total_qty,				--����������
			t4.return_memb_num,			--��������	
			t4.return_memb_sale, 		--�����۶�
			t4.return_memb_times 		--�����Ѵ���
			--,t4.return_memb_sale/t4.return_memb_num as return_memb_avg_sale		--�˾����۽��
			--,t4.return_memb_times/t4.return_memb_num as return_memb_avg_times	--�˾����۴���
		from t5_1 t5
		left join t4_1 t4
		on t5.AT_TEAR=t4.AT_TEAR
		and t5.SG_JM_FLAG=t4.SG_JM_FLAG
	 )
	 ,
	--ȡ����ȷֹ�˾������Ա�������۶���Ѵ���
	t4_2 as (
		select	AT_TEAR				--���
			,ADMS_ORG_NAME
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--��������
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--�������
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--�������
		from
		(
			select  
				 t1.AT_TEAR,				--���
				 t1.SG_JM_FLAG,
				 t1.ADMS_ORG_NAME,			--�ֹ�˾����
				 t2.member_id ,				--��һ���Ƿ���
				 t1.SALE_AMT	,			--���۶�
				 t1.sale_times				--���Ѵ���
			from t3 t1
			left join t3 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
			where t1.SG_JM_FLAG='NORMAL'		--ֻ�������չ����˵�
		)
		group by AT_TEAR
		,ADMS_ORG_NAME
	)
	,
	--�������ȷֹ�˾���ѻ�Ա����	  
	t5_2 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			count(1) as total_qty --���ѻ�Ա����	
		from t3
		group by AT_TEAR
		,ADMS_ORG_NAME
	 )
	 ,
	 --�õ�����ȷֹ�˾������Ա�������ѻ�Ա���������˾����Ѷ�����˾�����Ƶ��
	 t6_2 as 
	 (
		select 	 
			t5.AT_TEAR,					--���
			t5.ADMS_ORG_NAME,				--�Ƿ��չ�����
			t5.total_qty,				--����������
			t4.return_memb_num,			--��������	
			t4.return_memb_sale, 		--�����۶�
			t4.return_memb_times 		--�����Ѵ���
			--,t4.return_memb_sale/t4.return_memb_num as return_memb_avg_sale		--�˾����۽��
			--,t4.return_memb_times/t4.return_memb_num as return_memb_avg_times	--�˾����۴���
		from t5_2 t5
		left join t4_2 t4
		on t5.AT_TEAR=t4.AT_TEAR
		and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
	 )
	 ,
	--ȡ����ȸ�Ʒ�ิ����Ա�������۶���Ѵ���
	t4_3 as (
		select	AT_TEAR				--���
			,PROD_CATE_LEV2_NAME
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--��������
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--�������
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--�������
		from
		(
			select  
				 t1.AT_TEAR,				--���
				 t1.PROD_CATE_LEV2_NAME,
				 t2.member_id ,				--��һ���Ƿ���
				 t1.SALE_AMT	,			--���۶�
				 t1.sale_times				--���Ѵ���
			from t2_1 t1
			left join t2_1 t2
			on t1.AT_TEAR=t2.AT_TEAR+1
			and t1.member_id=t2.member_id
			and t1.PROD_CATE_LEV2_NAME=t2.PROD_CATE_LEV2_NAME
		)
		group by AT_TEAR
		,PROD_CATE_LEV2_NAME
	)
	,
	--�������ȸ�Ʒ�����ѻ�Ա����	  
	t5_3 as ( 
		select 
			AT_TEAR,
			PROD_CATE_LEV2_NAME,
			count(1) as total_qty --���ѻ�Ա����	
		from t2_1
		group by AT_TEAR,
			PROD_CATE_LEV2_NAME
	 )
	 ,
	 --�õ�����ȸ�����Ա�������ѻ�Ա���������˾����Ѷ�����˾�����Ƶ��
	 t6_3 as 
	 (
		select 	 
			t5.AT_TEAR,					--���
			T5.PROD_CATE_LEV2_NAME,
			t5.total_qty,				--����������
			t4.return_memb_num,			--��������	
			t4.return_memb_sale, 		--�����۶�
			t4.return_memb_times 		--�����Ѵ���
		from t5_3 t5
		left join t4_3 t4
		on t5.AT_TEAR=t4.AT_TEAR
		AND T5.PROD_CATE_LEV2_NAME=T4.PROD_CATE_LEV2_NAME
	 )
	  ,
	--ȡ���ֹ�˾�¶ȸ�����Ա�������۶���Ѵ���
	t4_4 as (
		select	AT_TEAR							--�¶�
			,AT_MONTH
			,ADMS_ORG_NAME
			,sum(IS_CB_FLAG) as return_memb_num	--��������
			,sum(case when IS_CB_FLAG =1 then SALE_AMT else 0 end) as return_memb_sale	--�������
			,sum(case when IS_CB_FLAG =1 then sale_times else 0 end) as return_memb_times	--��������
		from
		(
			select  
				 t1.AT_TEAR,				--���
				 t1.AT_MONTH,
				 t1.SG_JM_FLAG,
				 t1.IS_CB_FLAG,				--�Ƿ񸴹�
				 t1.ADMS_ORG_NAME,			--�ֹ�˾����
				 t1.SALE_AMT	,			--���۶�
				 t1.sale_times				--���Ѵ���
			from t3_2 t1
			
		)
		group by AT_TEAR
		,AT_MONTH
		,ADMS_ORG_NAME
	)
	,
	--���������ȷֹ�˾���ѻ�Ա����	  
	t5_4 as ( 
		select 
			AT_TEAR,
			ADMS_ORG_NAME,
			count(distinct member_id) as total_qty --���ѻ�Ա����	
		from t3_2
		group by AT_TEAR
		,ADMS_ORG_NAME
	 )
	 ,
	 --�õ����¶ȷֹ�˾������Ա�������ѻ�Ա���������˾����Ѷ�����˾�����Ƶ��
	 t6_4 as 
	 (
		select 	
			t4.AT_TEAR,
			t4.AT_MONTH,
			t4.ADMS_ORG_NAME,				--�ֹ�˾
			t4.return_memb_num,			--��������	
			t4.return_memb_sale, 		--�����۶�
			t4.return_memb_times, 		--�����Ѵ���
			t5.total_qty				--����������
		from t4_4 t4
		left join t5_4 t5
		on t4.AT_TEAR=t5.AT_TEAR+1
		and t5.ADMS_ORG_NAME=t4.ADMS_ORG_NAME
	 )
	 
	 select * from t6_2