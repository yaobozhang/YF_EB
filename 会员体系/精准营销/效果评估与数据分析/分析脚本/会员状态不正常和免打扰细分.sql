分析内容做下补充：
1、会员状态“不正常”的会员，计算不正常状态分类及对应人数和占比； 
	select 
		STATE
		,count(MEMB_CODE) as nums
	from   DW.FACT_MEMBER_BASE 
	--where STATE !='EFC'
	group by  STATE
	order by nums desc

--SELECT * FROM DS_POS.SYS_DICT F WHERE F.TYPE_CODE = 'customerStatus' AND F.DELETELABLE = '0'


2、2018年期间开卡的会员，当前手机号码不正常的会员，计算会员注册方式+会员来源对应的人数及占比；
/*--手机号码不正常的人数 及来源
	with t1 as (
		select MEMB_CODE
		,MEMB_SOUR --会员来源
		from  DW.FACT_MEMBER_BASE 
		where CREA_TIME >='20180101' and  CREA_TIME<'20190101' 
		and MEMB_MOBI not  like_regexpr '^1[3578]\d{9}$'
	)

	--select  count(*) from t1 
	,t2 as (
		select    
			MEMB_CARD_CODE  --会员编号
			,REGI_TYPE  --注册方式  
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

--手机号码不正常的人数 及来源
	with t1 as (
		select MEMB_CODE
		,MEMB_SOUR --会员来源
		,APPLY_TYPE
		from  DW.FACT_MEMBER_BASE 
		where CREA_TIME >='20180101' and  CREA_TIME<'20190101' 
		and MEMB_MOBI not  like_regexpr --'^1[3578]\d{9}$'
		'^(((13[0-9])|(14[579])|(15([0-3]|[5-9]))|(16[6])|(17[0135678])|(18[0-9])|(19[89]))\d{8})$'
	)



--字典表查找
3、近一年有消费的免打扰用户，分别计算分公司、性别和年龄段的分布情况；
--筛选免打扰会员 
with t1 as (
	select  
		MEMB_CODE --会员编码
		,BELONG_PHMC_CODE--所属门店编码
		--,IS_NO_DIST --是否免打扰
	 from  DW.FACT_MEMBER_BASE 
	where IS_NO_DIST='1'  --免打扰
)
--select count(*) from t1  --3,040,727  不可打扰会员

--取近一年有消费的会员
,t2 as(
	select MEMBER_ID,GNDR_AGE_TYPE
	from (
		select 
			MEMBER_ID
			,GNDR_AGE_TYPE  --性别年龄分段
		  ,DAYS_BETWEEN(LAST_TIME_CUNSU_DATE,to_char('20190104','yyyymmdd')) as R_daydiff--最近一次消费时间间隔
		from  DM.FACT_MEMBER_CNT_INFO
		where DATA_DATE='20190101'  
	) where R_daydiff <=365
)
--select count(*) from t2 --11,065,129  近一年有消费的会员

--合并  得到近一年有消费的免打扰会员
,t3  as (
	select  MEMB_CODE
			,BELONG_PHMC_CODE --所属门店编码
			,GNDR_AGE_TYPE
			,case when GNDR_AGE_TYPE='0101' then '男青年'
			when GNDR_AGE_TYPE='0201' then '女青年'
			when GNDR_AGE_TYPE='0102' then '男中年'
			when GNDR_AGE_TYPE='0202' then '女中年'
			when GNDR_AGE_TYPE='0103' then ' 男老年'
			when GNDR_AGE_TYPE='0203' then '女老年'
			else  '其他'  end as GNDR_AGE_TYPE_DESC
	from t1 
	inner join  t2 
	on t1.MEMB_CODE=t2.MEMBER_ID
)
--select count(*) from t3  --1,250,441

--关联分公司
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
			,BELONG_PHMC_CODE --所属门店编码
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
		ADMS_ORG_CODE  --分公司编码
		,ADMS_ORG_NAME --分公司名称
		,GNDR_AGE_TYPE --性别年龄段分布
		,GNDR_AGE_TYPE_DESC
		,count(MEMB_CODE) as nums 
from t5
group  by  ADMS_ORG_CODE ,ADMS_ORG_NAME ,GNDR_AGE_TYPE,GNDR_AGE_TYPE_DESC
order  by nums desc 













