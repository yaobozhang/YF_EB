with t1 as(
	select 
		MEMB_CARD_CODE	--会员卡号
		,MEMB_CODE	--会员编码
		,MEMB_NAME	--会员名称
		,MEMB_MOBI	--会员手机号
		,MEMB_LAND_NUM	--会员座机编号
		,BELONG_PHMC_CODE	--所属门店编码
		,IS_NO_DIST	--是否免打扰   0 -/1 哪个是免打扰
	from DW.FACT_MEMBER_BASE
	where IS_NO_DIST='0'	--非免打扰
)
,t2 as(--韶光市乡亲大药房信息
	select 
		PHMC_CODE   	--门店编码
		,PHMC_S_NAME	--门店名称 
	from DW.DIM_PHMC 
	where  PHMC_F_NAME  like '%韶关市%乡亲%'
	group by PHMC_CODE,PHMC_S_NAME
)
, t3 as (
	select
			MEMB_CARD_CODE 	--会员编号 --ID			
			,MEMB_CODE	    --会员卡号
			,t2.PHMC_CODE   --门店编码 		
			,t2.PHMC_S_NAME	--门店名称
			,MEMB_NAME  	--姓名
			,MEMB_MOBI 		--手机号码
			,MEMB_LAND_NUM   --会员座机编号
		from t1
		inner join t2 
		on t1.BELONG_PHMC_CODE=t2.PHMC_CODE 
) 
--select count(*) from t3 ;--12,168
--取开卡时间在10-12月得会员
,t4 as (
	select 
		MEMBER_ID
	from  DM.FACT_MEMBER_CNT_INFO
	where DATA_DATE='20190101' and OPEN_CARD_DAYS<=92
)
--select count(*) from t4;--6,968,021
,t5 as(
	select 
			MEMB_CARD_CODE 	--会员编号 --ID			
			,MEMB_CODE	    --会员卡号
			,PHMC_CODE   --门店编码 		
			,PHMC_S_NAME	--门店名称
			,MEMB_NAME  	--姓名
			,MEMB_MOBI 		--手机号码
			,MEMB_LAND_NUM   --会员座机编号
	from  t3 
	inner join t4
	on t3.MEMB_CODE=t4.MEMBER_ID
)

--select * from t5
select count(*) from t5
--select count(*)  from t5  ;--5,651

 
