create column table ext_tmp.sale_trend_20181119_01
as 
(select t1.member_id,sale_date,sale_money,gross_money,
row_number() over(partition by t1.member_id order by sale_date asc ) as rn 
from 
 (select member_id,to_char(sale_time,'yyyymmdd') as sale_date,
 sum(sale_money) as sale_money ,sum(gross_money) as  gross_money
 from 
 (select member_id,sale_bill_code,sale_time
  from  ds_pos.sales_order
  where  sale_time > '20171031'
  and sale_time<current_date
  and member_id is not null) s1 
 inner join 
  (select sale_bill_code,sale_money,account_price_gross_money as  gross_money,quantity
              from  ds_pos.sales_orderdetails
              where  sale_time > '20171031'
     and sale_time<current_date
  ) s2 
  on s1.sale_bill_code=s2.sale_bill_code
  group by member_id,to_char(sale_time,'yyyymmdd') 
 ) t1 
 inner join 
 (select memb_card_code as member_id,OPEN_CARD_DATE from  DM.FACT_MEMB_LABEL_CURR 
where to_char(OPEN_CARD_DATE,'yyyymmdd')>'20171031' ) t2 
 on t1.member_id=t2.member_id
 where sale_date>=to_char(open_card_date,'yyyymmdd') 
 );
 
 
-- 客单价分布情况
 select t1.rn,percent_10,
		t1.percent_20,
		t1.percent_30,
		t1.percent_40,
		t1.percent_50,
		t1.percent_60,
		t1.percent_70,
		t1.percent_80,
		t1.percent_90,
		t1.percent_99,
		t2.avg_sale_money

 from (
(select distinct rn,
PERCENTILE_DISC (0.10) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_10,
PERCENTILE_DISC (0.20) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_20, 
PERCENTILE_DISC (0.30) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_30, 
PERCENTILE_DISC (0.40) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_40, 
PERCENTILE_DISC (0.50) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_50,
PERCENTILE_DISC (0.60) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_60,
PERCENTILE_DISC (0.70) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_70,
PERCENTILE_DISC (0.80) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_80,
PERCENTILE_DISC (0.90) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_90,
PERCENTILE_DISC (0.99) WITHIN GROUP ( ORDER BY sale_money ASC) over(partition by rn )  percent_99
from 
(select rn,sale_money from ext_tmp.sale_trend_20181119_01 where rn<=30))t1
left join 
(select rn,avg(sale_money) as avg_sale_money from ext_tmp.sale_trend_20181119_01
group by rn)t2
on t1.rn=t2.rn)
 
 
 --毛利率分布情况
select distinct rn,
PERCENTILE_DISC (0.10) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_10,
PERCENTILE_DISC (0.20) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_20, 
PERCENTILE_DISC (0.30) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_30, 
PERCENTILE_DISC (0.40) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_40, 
PERCENTILE_DISC (0.50) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_50,
PERCENTILE_DISC (0.60) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_60,
PERCENTILE_DISC (0.70) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_70,
PERCENTILE_DISC (0.80) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_80,
PERCENTILE_DISC (0.90) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_90,
PERCENTILE_DISC (0.99) WITHIN GROUP ( ORDER BY gross_money_pct ASC) over(partition by rn )  percent_99
from 
(select rn,
case when sale_money=0  then 0
	 when sale_money<>0 then round(gross_money/(sale_money+0.00001),3)
	 end as gross_money_pct
from ext_tmp.sale_trend_20181119_01 where rn<=30)t


-- 平均值
select t.rn,avg(t.gross_money_pct) as avg_gross_money_pct  ---平均毛利率
from 
(select rn,
case when sale_money=0  then 0
	 when sale_money<>0 then round(gross_money/(sale_money+0.00001),3)
	 end as gross_money_pct
from ext_tmp.sale_trend_20181119_01 where rn<=30)t
group by rn
order by t.rn;
 
 
 