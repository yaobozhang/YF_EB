
select 
to_date(a.sale_time),
coupon_code,
ADMS_ORG_NAME,
ADMS_ORG_CODE,
phmc_code,
PHMC_F_NAME,
count(distinct a.sale_bill_code),
sum(sale_money),
sum(ACCOUNT_PRICE_GROSS_MONEY)
 from "DS_POS"."SALES_ORDER" a 
 INNER JOIN "DS_POS"."SALES_ORDERDETAILS" b
 on a.sale_bill_code=b.sale_bill_code
left join "DW"."DIM_PHMC" c
on a.store_code=c.phmc_code.
 where coupon_code='100200382'
 and to_date(a.sale_time)='20181111'
group by to_date(a.sale_time),
coupon_code,
ADMS_ORG_NAME,
ADMS_ORG_CODE,
phmc_code,
PHMC_F_NAME