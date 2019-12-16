�����������²��䣺
1����Ա״̬�����������Ļ�Ա�����㲻����״̬���༰��Ӧ������ռ�ȣ� 
	select 
		STATE
		,count(MEMB_CODE) as nums
	from   DW.FACT_MEMBER_BASE 
	--where STATE !='EFC'
	group by  STATE
	order by nums desc

--SELECT * FROM DS_POS.SYS_DICT F WHERE F.TYPE_CODE = 'customerStatus' AND F.DELETELABLE = '0'


2��2018���ڼ俪���Ļ�Ա����ǰ�ֻ����벻�����Ļ�Ա�������Աע�᷽ʽ+��Ա��Դ��Ӧ��������ռ�ȣ�
/*--�ֻ����벻���������� ����Դ
	with t1 as (
		select MEMB_CODE
		,MEMB_SOUR --��Ա��Դ
		from  DW.FACT_MEMBER_BASE 
		where CREA_TIME >='20180101' and  CREA_TIME<'20190101' 
		and MEMB_MOBI not  like_regexpr '^1[3578]\d{9}$'
	)

	--select  count(*) from t1 
	,t2 as (
		select    
			MEMB_CARD_CODE  --��Ա���
			,REGI_TYPE  --ע�᷽ʽ  
			,MEMB_SOUR
		 from  DW.FACT_MEMBER_BASE_INFO

	)
	,t3 as (
		select   t1.MEMB_SOUR
				,REGI_TYPE
				,count(MEMB_CODE) as nums
		from t1 
		left  join  t2 
		on  t1.MEMB_CODE=t2.MEMB_CARD_CODE
		group  by  t1.MEMB_SOUR ,REGI_TYPE
	)
	*/

--�ֻ����벻���������� ����Դ
	with t1 as (
		select MEMB_CODE
		,MEMB_SOUR --��Ա��Դ
		,APPLY_TYPE
		from  DW.FACT_MEMBER_BASE 
		where CREA_TIME >='20180101' and  CREA_TIME<'20190101' 
		and MEMB_MOBI not  like_regexpr --'^1[3578]\d{9}$'
		'^(((13[0-9])|(14[579])|(15([0-3]|[5-9]))|(16[6])|(17[0135678])|(18[0-9])|(19[89]))\d{8})$'
	)



--�ֵ�����
3����һ�������ѵ�������û����ֱ����ֹ�˾���Ա������εķֲ������
--ɸѡ����Ż�Ա 
with t1 as (
	select  
		MEMB_CODE --��Ա����
		,BELONG_PHMC_CODE--�����ŵ����
		--,IS_NO_DIST --�Ƿ������
	 from  DW.FACT_MEMBER_BASE 
	where IS_NO_DIST='1'  --�����
)
--select count(*) from t1  --3,040,727  ���ɴ��Ż�Ա

--ȡ��һ�������ѵĻ�Ա
,t2 as(
	select MEMBER_ID,GNDR_AGE_TYPE
	from (
		select 
			MEMBER_ID
			,GNDR_AGE_TYPE  --�Ա�����ֶ�
		  ,DAYS_BETWEEN(LAST_TIME_CUNSU_DATE,to_char('20190104','yyyymmdd')) as R_daydiff--���һ������ʱ����
		from  DM.FACT_MEMBER_CNT_INFO
		where DATA_DATE='20190101'  
	) where R_daydiff <=365
)
--select count(*) from t2 --11,065,129  ��һ�������ѵĻ�Ա

--�ϲ�  �õ���һ�������ѵ�����Ż�Ա
,t3  as (
	select  MEMB_CODE
			,BELONG_PHMC_CODE --�����ŵ����
			,GNDR_AGE_TYPE
			,case when GNDR_AGE_TYPE='0101' then '������'
			when GNDR_AGE_TYPE='0201' then 'Ů����'
			when GNDR_AGE_TYPE='0102' then '������'
			when GNDR_AGE_TYPE='0202' then 'Ů����'
			when GNDR_AGE_TYPE='0103' then ' ������'
			when GNDR_AGE_TYPE='0203' then 'Ů����'
			else  '����'  end as GNDR_AGE_TYPE_DESC
	from t1 
	inner join  t2 
	on t1.MEMB_CODE=t2.MEMBER_ID
)
--select count(*) from t3  --1,250,441

--�����ֹ�˾
,t4 as (
	select  
		PHMC_CODE  
		,ADMS_ORG_CODE
		,ADMS_ORG_NAME
	from  DW.DIM_PHMC
)
,t5 as (
	select  
	 		MEMB_CODE
			,BELONG_PHMC_CODE --�����ŵ����
			,PHMC_CODE  
			,GNDR_AGE_TYPE
			,GNDR_AGE_TYPE_DESC
			,ADMS_ORG_CODE
			,ADMS_ORG_NAME
	from t3
	left join t4
	on t3.BELONG_PHMC_CODE=t4.PHMC_CODE
)
--select  count(*) from t5--1,250,441
select  
		ADMS_ORG_CODE  --�ֹ�˾����
		,ADMS_ORG_NAME --�ֹ�˾����
		,GNDR_AGE_TYPE --�Ա�����ηֲ�
		,GNDR_AGE_TYPE_DESC
		,count(MEMB_CODE) as nums 
from t5
group  by  ADMS_ORG_CODE ,ADMS_ORG_NAME ,GNDR_AGE_TYPE,GNDR_AGE_TYPE_DESC
order  by nums desc 













