
-- 截止18年10月31日会员人数
select count(*) from DM.FACT_MEMBER_CNT_INFO where data_date='2018-10-31'

--会员销售额-订单表中
select sum(account_money) as memb_account_money from ds_pos.sales_order
 where sale_time>=to_date('20171101','yyyymmdd') and sale_time<=to_date('20181031','yyyymmdd')
 and member_id<>'';
 
 --全系统销售额
 select sum(account_money) as all_account_money from ds_pos.sales_order
 where sale_time>=to_date('20171101','yyyymmdd') and sale_time<=to_date('20181031','yyyymmdd')
 
 -- 17年11月1日-18年10月31日有消费会员数
 select count(*) from (
select distinct member_id from ds_pos.sales_order 
where sale_time>=to_date('20171101','yyyymmdd') and sale_time<=to_date('20181031','yyyymmdd') 
and member_id is not null);

--截止17年年底活跃会员人数--会员表
1730350	
select count(*) from FACT_MEMBER_CNT_INFO where data_date='2018-10-31' and IS_ACTIV_MEMB=1;



--会员销售金额，销售毛利--订单表
create column table ext_tmp.huiyuan_sal_20181121_01 as
 (select a.member_id,
		 sum(b.gross_money) as gross_money,
		 sum(b.sale_money) as sale_money
 from 
	(select member_id,sale_bill_code from ds_pos.sales_order 
	where sale_time>=to_date('20171101','yyyymmdd') and sale_time<=to_date('20181031','yyyymmdd')
	 and member_id<>'')a
join 
	(select sale_bill_code,sum(sale_money) as sale_money,
	sum(account_price_gross_money) as gross_money
	 from ds_pos.SALES_ORDERDETAILS
	where sale_time>=to_date('20171101','yyyymmdd') and sale_time<=to_date('20181031','yyyymmdd')
	group by sale_bill_code)b
on a.sale_bill_code=b.sale_bill_code
 group by  a.member_id 
);


create column table ext_tmp.huiyuan_sal_20181121_02 as
(
select member_id,gross_money,sale_money,
	sum(gross_money) over (order by gross_cnt asc) as sum_gross_money,
	sum(sale_money) over (order by sale_cnt asc) as sum_sale_money
from 

(
select member_id,gross_money,sale_money,
	row_number() over(order  by gross_money desc)as gross_cnt,
	row_number() over(order  by sale_money desc)as sale_cnt
from ext_tmp.huiyuan_sal_20181121_01
)
)

--计算--
with a1 as 
(select member_id,gross_money,sale_money,sum_gross_money,sum_money_money,all_gross_money,
sum_gross_money/all_gross_money as all_gross_money_pct,
sum_money_money/all_sale_money as all_sale_money_pct
from  ext_tmp.huiyuan_sal_20181121_02 t1 
left join 
	(select sum(gross_money) as all_gross_money,
	sum(sale_money) as all_sale_money
	from ext_tmp.huiyuan_sal_20181121_02 
	)  t2 
on 1=1
)
select count(member_id) as member_cnt,sum(gross_money) as gross_money,sum(sale_money) as sale_money 
from a1 
where all_sale_money_pct<=0.8













