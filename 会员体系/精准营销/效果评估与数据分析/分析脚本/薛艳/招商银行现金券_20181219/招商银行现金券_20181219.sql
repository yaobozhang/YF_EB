
select 
e.ADMS_ORG_NAME,
e.phmc_code,
e.PHMC_F_NAME,
c.sale_bill_code,
c.sale_time,
d.sale_money,
b.coupon_money
 
 from "DS_CRM"."ZT_COUPON" a
 inner join "DS_CRM"."ZT_COUPON_TEMPLATE"  b
 on a.TEMPLATE_ID=b.uuid
 inner join "DS_POS"."SALES_ORDER" c
 on a.order_code=c.sale_bill_code
 inner join "DS_POS"."SALES_ORDERDETAILS" d
 on c.sale_bill_code=d.sale_bill_code
 left join "DW"."DIM_PHMC" e
 on c.store_code=e.phmc_code
 where a.TEMPLATE_ID='5bff9b7ae55eb41f00fcda81'
 and to_date(c.sale_time)>='20181201'
 and to_date(c.sale_time)<='20181228'