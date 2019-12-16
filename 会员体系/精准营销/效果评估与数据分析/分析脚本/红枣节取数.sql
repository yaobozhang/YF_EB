---人群划分
with t1 as (
select t1.member_id,count(distinct t1.sale_bill_code) as order_cnt,sum(sale_money) as sale_money,sum(gross_money)  as gross_money
from(
(select 
			member_id
			,sale_bill_code
			,sale_time  
	from ds_pos.sales_order
	where sale_time >'20170930'
	and sale_time<'20181001'
	and member_id is not null ) t1   
	inner join 
		(	select 
				sale_bill_code  ---订单号
				,good_code       ---商品编码
				,sale_money      ---销售额
				,account_price_gross_money as gross_money  ---毛利额
			from  ds_pos.sales_orderdetails
            where sale_time >'20170930'
			and sale_time<'20181001'
			and good_code='2042055'
		) t2
		on t1.sale_bill_code=t2.sale_bill_code 
) group by t1.member_id
)
,t2 as (
	select  member_id
			,sale_money 
			,gross_money
			,case when order_cnt=1 then '1'
			when order_cnt=2 then '2'
			when order_cnt>=3 then '3'
			else null end as order_cnt 
			,case when order_cnt=1 then '购买1次'
			when order_cnt=2 then '购买2次'
			when order_cnt>=3 then '购买3次及以上'
			else null end as order_cnt_desc   
			from t1 
)
,t3 as (
	select order_cnt,order_cnt_desc,sum(sale_money) as sale_money,sum(gross_money) as gross_money
	from  t2
	where order_cnt is not null
	group by order_cnt,order_cnt_desc
)
,t4 as (
select member_id,t3.order_cnt,t3.order_cnt_desc,t3.sale_money,t3.gross_money
from t2
inner join 
t3
on t2.order_cnt=t3.order_cnt
)


--select * from t4


---人群匹配  选出在10.1-10.15日内再次购买红枣的顾客的的销售  按购买次数匹配
, t5 as (
select t1.member_id,count(distinct t1.sale_bill_code) as order_cnt,sum(sale_money) as sale_money,sum(gross_money)  as gross_money
from(
(select 
			member_id
			,sale_bill_code
			,sale_time  
	from ds_pos.sales_order
	where sale_time > ADD_DAYS('20180930',1)
	and sale_time<ADD_DAYS('20181016',1)
	and member_id is not null ) t1   
	inner join 
		(	select 
				sale_bill_code  ---订单号
				,good_code       ---商品编码
				,sale_money      ---销售额
				,account_price_gross_money as gross_money  ---毛利额
			from  ds_pos.sales_orderdetails
            where sale_time > ADD_DAYS('20180930',1)
			and sale_time<ADD_DAYS('20181016',1)
			and good_code='2042055'
		) t2
		on t1.sale_bill_code=t2.sale_bill_code 
) group by t1.member_id
)
,t6 as (
	select  member_id
			,sale_money 
			,gross_money 
			,case when order_cnt=1 then '1'
			when order_cnt=2 then '2'
			when order_cnt>=3 then '3'
			else null end as order_cnt 
			,case when order_cnt=1 then '购买1次'
			when order_cnt=2 then '购买2次'
			when order_cnt>=3 then '购买3次及以上'
			else null end as order_cnt_desc   
			from t5
)

--select count(member_id) from t6



---人群匹配  选出在10.1-10.15日内再次购买红枣的顾客的的销售  按购买次数匹配
,t7 as (
select 
t2.member_id,t6.sale_money  ,t6.gross_money ,t6.order_cnt,t6.order_cnt_desc
from t6
inner join 
t2
on t2.member_id=t6.member_id
)

--按购买次数计算销售额 毛利额
,t8 as (
select order_cnt,order_cnt_desc,sum(sale_money) as sale_money,sum(gross_money) as gross_money
	from  t7
	where order_cnt is not null
	group by order_cnt,order_cnt_desc
)

select t8.order_cnt,t8.order_cnt_desc, t3.sale_money as "单品销售额", t3.gross_money as "单品毛利额",
t8.sale_money as "回头销售额", t8.gross_money as "回头毛利额"
from t8 
inner join
t3
on t8.order_cnt=t3.order_cnt






