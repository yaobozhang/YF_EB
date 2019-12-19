--20191218
--by ybz
--主题：日常观测当前会员数据

--step1:首先，得到会员基本数据
with t1 as (
	select t1.MEMB_CODE as member_id
		,t2.member_id as if_buy_lastyear
	from "DW"."FACT_MEMBER_BASE" t1
	left join 
	(
		select member_id
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191218'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20181218'))
		group by member_id
	) t2
	on t1.memb_code=t2.member_id
)
,

--总会员数，活跃会员数
t2 as (
	SELECT COUNT(MEMBER_ID) as memb_num
		,count(if_buy_lastyear) as memb_active_num
	FROM t1 

)
,
--慢病门店数，慢病建档会员数
--慢病门店数从业务方拿
--拿到慢病建档人数
t3 as (
	select count(distinct customer_id) AS CHRONIC_NUM from "DS_ZT"."CHRONIC_PATIENT"
)
select t2.*,t3.* from t2 left join t3 on 1=1