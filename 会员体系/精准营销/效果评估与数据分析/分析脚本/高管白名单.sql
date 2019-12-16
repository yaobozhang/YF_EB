--高管白名单脚本

with  t1 as(
	SELECT  
		MEMB_NAME
		,MEMB_MOBI	--会员手机号	
	 from "EXT_TMP".WHITE_NUMS
)

--select count(*) from t1  62 
,t2 as(
	select 
		MEMB_CARD_CODE	--会员卡号
		,MEMB_CODE	--会员编码
		,MEMB_NAME	--会员名称
		,MEMB_MOBI	--会员手机号	
	from DW.FACT_MEMBER_BASE
)
,t3 as(
	select 
		t1.MEMB_NAME
		,t1.MEMB_MOBI
		,t2.MEMB_CODE      --会员编码
		,t2.MEMB_CARD_CODE	--会员卡号
	from t1 
	left join t2
	on t1. MEMB_MOBI=t2.MEMB_MOBI  --and t1. MEMB_NAME=t2.MEMB_NAME 
)

select * from t3
--select count(*) from t3
注意： 像电话号码这类的数值型字段 在导入hana表的时候 要记得将字段转化为文本格式。