--贡献者：姚泊彰
--时间：20200102
--目的：用于抽样会员，得到每个会员的消费行为路径

--首先，得到成长值>3000的会员
with t1 as (
	
	select member_id 
	from "EXT_TMP"."YBZ_MEMBER_ANY"
	group by member_id 
	having sum(sale_amt) >1000
	limit 100			--拿1000个
)
,
--得到选择会员全消费路径
t2 as (
	select member_id
		,stsc_date
		,GOODS_NAME
		,SELF_CATE_LEV2_NAME
		,SALE_QTY
	from "EXT_TMP"."YBZ_MEMBER_ANY" t1
	where exists(
		select 1 from t1 t2
		where t1.member_id=t2.member_id
	)
)
,
--得到会员年龄等信息
t3 as (
	select t1.member_id
		,t1.stsc_date
		,t1.GOODS_NAME
		,t1.SELF_CATE_LEV2_NAME
		,t1.SALE_QTY
		,'2018'-to_char(t2.birt_date,'YYYY') AS AGE
	from t2 t1
	left join
	dw.fact_member_base t2
	on t1.member_id=t2.memb_code

)
select * from t2 order by member_id,stsc_date asc