call EXT_TMP.PW_COMPANY_POINT();

alter  PROCEDURE EXT_TMP.PW_COMPANY_POINT()
LANGUAGE SQLSCRIPT 
SQL SECURITY INVOKER
AS 
var_sql NVARCHAR (32);
BEGIN  
var_sql='1=1';
--门店基础信息
var0= 
select 
case when PROP_ATTR in ('Z02','Z07') THEN  company_name --收购
WHEN PROP_ATTR in ('Z03','Z04') THEN '加盟'  --加盟
when PROP_ATTR in ('Z01','Z06','Z08') THEN adms_org_name --直营
end  adms_org_name
,case when PROP_ATTR in ('Z02','Z07') THEN  '收购' --收购
WHEN PROP_ATTR in ('Z03','Z04') THEN '加盟'  --加盟
when PROP_ATTR in ('Z01','Z06','Z08') THEN '直营' --直营
end  PROP_ATTR
,CLOSE_DATE
,NEW_PHMC_CODE
,phmc_code
from
dw.dim_phmc
where PROP_ATTR in ('Z02','Z07','Z03','Z04','Z01','Z06','Z08')

;
--会员基础信息
var01=
select 
customer_id
,case when create_time<add_years(now(),-1) then '老客' else '新客' end as "fl"
,at_store
from 
"DS_CRM"."TP_CU_CUSTOMERBASE" c
where c.STATE='EFC'--取有效会员
;


--品类划分
var_cate=
select 
goods_sid
,cate_fl
from
(
	select
	goods_sid 
--糖尿病和心脑血管品类，健康品类主要指保健品、养生中药、医疗器械品类
	,case when PROD_CATE_LEV1_NAME ='保健食品' then '健康品类'
	when PROD_CATE_LEV1_NAME ='中药' then '健康品类'
	when PROD_CATE_LEV1_NAME ='医疗器械' then '健康品类'
	when PROD_CATE_LEV2_NAME in ('心脑血管用药','糖尿病用药') then '慢病品类'
	end cate_fl
	from 
	"DW"."DIM_GOODS_H"
)
;

--上一年有消费会员销售
var_sale_1=
select
member_id
,sum(bt)bt
,sum(sale_amt)sale_amt
,sum(mb_bt)mb_bt
,sum(mb_sale_amt)mb_sale_amt
,sum(bjsp_bt)bjsp_bt
,sum(bjsp_sale_amt)bjsp_sale_amt
from
(
	select 
	member_id
	,t.stsc_date,phmc_code
	,1 bt
	,sum(sale_amt)sale_amt
	,max(case when (cate_fl='慢病品类')then 1 else 0 end)mb_bt
	,sum(case when (cate_fl='慢病品类')then sale_amt else 0 end)mb_sale_amt
	,max(case when (cate_fl='健康品类')then 1 else 0 end)bjsp_bt
	,sum(case when (cate_fl='健康品类')then sale_amt else 0 end)bjsp_sale_amt
	from "DW"."FACT_SALE_ORDR_DETL" t
	left join :var_cate t1 on t.goods_sid=t1.goods_sid
	where stsc_date>=add_years(now(),-1)
	and member_id is not null
	and CV_FLAG='Y'
	and (ORDR_CATE_CODE<>2 or ORDR_CATE_CODE is null)
	group by member_id
	,stsc_date,phmc_code
)
group by member_id
;
--上两年有消费会员销售
var_sale_2=
select
member_id
,sum(bt)bt
,sum(sale_amt)sale_amt
,sum(mb_bt)mb_bt
,sum(mb_sale_amt)mb_sale_amt
,sum(bjsp_bt)bjsp_bt
,sum(bjsp_sale_amt)bjsp_sale_amt
from
(
	select 
	member_id
	,t.stsc_date
	,1 bt
	,phmc_code
	,sum(sale_amt)sale_amt
	,max(case when (cate_fl='慢病品类')then 1 else 0 end)mb_bt
	,sum(case when (cate_fl='慢病品类')then sale_amt else 0 end)mb_sale_amt
	,max(case when (cate_fl='健康品类')then 1 else 0 end)bjsp_bt
	,sum(case when (cate_fl='健康品类')then sale_amt else 0 end)bjsp_sale_amt
	
	from "DW"."FACT_SALE_ORDR_DETL" t
	left join :var_cate t1 on t.goods_sid=t1.goods_sid
	where stsc_date>add_years(now(),-2)
	and stsc_date<=add_years(now(),-1)
	and member_id is not null
	and CV_FLAG='Y'
	and (ORDR_CATE_CODE<>2 or ORDR_CATE_CODE is null)
	group by member_id
	,stsc_date
	,phmc_code
)
group by member_id
;

---------按分公司统计
--公司门店统计
var1=
select 
PROP_ATTR PROP_ATTR1
,adms_org_name adms_org_name1
,count(1)"门店数" --门店数
,count(case when CLOSE_DATE>now() or CLOSE_DATE is null then phmc_code end ) "未关停门店数" --未关停门店数
from :var0 t
where phmc_code=NEW_PHMC_CODE --剔出门店编码变化的门店（旧门店）
group by PROP_ATTR
,adms_org_name
;
--公司会员统计

var2=
select 
t.PROP_ATTR
,adms_org_name
,count(customer_id)all_mt --总会员数
,count(case when "fl"='老客' then customer_id end ) lk_mt --老客数
,case when count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end )=0 then 0 else count(case when "fl"='老客' then t1.member_id end )/count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end ) end  as "平均每店老客消费人数"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else count(case when "fl"='老客' then  t2.member_id end )/count(case when "fl"='老客' then t1.member_id end )end "老客年复购率"
,case when count(case when "fl"='老客' then t1.member_id end)=0 then 0 else sum(case when "fl"='老客' then t1.bt end )/count(case when "fl"='老客' then t1.member_id end) end as "老客年消费频次"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else sum(case when "fl"='老客' then  t1.sale_amt end )/count(case when "fl"='老客' then t1.member_id end ) end  as "年产值"
,case when count(c.customer_id)=0 then 0 else  count(case when "fl"='新客' then c.customer_id end )/count( customer_id) end  "新增会员人数占比"
,case when count(case when "fl"='新客' then c.customer_id end )=0 then 0 else count(case when "fl"='新客' and t1.member_id is not null then c.customer_id end )/count(case when "fl"='新客' then c.customer_id end ) end "转化率"
,case when count(case when "fl"='新客' then t1.member_id end)=0 then 0 else sum(case when "fl"='新客' then t1.bt end )/count(case when "fl"='新客' then t1.member_id end) end as "新客年消费频次"
,case when count(case when "fl"='新客' then t1.member_id end )=0 then 0 else sum(case when "fl"='新客' then  t1.sale_amt end )/count(case when "fl"='新客' then t1.member_id end ) end  "新客年产值"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.mb_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店慢病消费人数"
,case when count(case when t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.mb_sale_amt>0 then t2.member_id end )/count(case when t1.mb_sale_amt>0 then t1.member_id end )end  "慢病品类年复购率"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_bt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年消费频次"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_sale_amt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年产"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.bjsp_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店健康品类消费人数"
,case when count(case when t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.bjsp_sale_amt>0 then t2.member_id end )/count(case when t1.bjsp_sale_amt>0 then t1.member_id end )end  "健康品类年复购率"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_bt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年消费频次"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_sale_amt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年产"
from 
:var01 c
inner join :var0 t on c.at_store=t.phmc_code
left join :var_sale_1 t1 on c.customer_id=t1.member_id
left join :var_sale_2 t2 on t1.member_id=t2.member_id
--where c.STATE='EFC' --取有效会员
group by t.PROP_ATTR
,adms_org_name
;
var31=
select * 
from :var1 t
left join :var2 t1 on t.adms_org_name1=t1.adms_org_name  and t.PROP_ATTR1=t1.PROP_ATTR
;
---------按分公司统计



---------按分收购统计
--公司门店统计
var11=
select 
PROP_ATTR PROP_ATTR1
,'合计'adms_org_name1
,count(1)"门店数" --门店数
,count(case when CLOSE_DATE>now() or CLOSE_DATE is null then phmc_code end ) "未关停门店数" --未关停门店数
from :var0 t
where phmc_code=NEW_PHMC_CODE --剔出门店编码变化的门店（旧门店）
group by PROP_ATTR
;
--公司会员统计

var21=
select 
PROP_ATTR 
,'合计'adms_org_name
,count(customer_id)all_mt --总会员数
,count(case when "fl"='老客' then customer_id end ) lk_mt --老客数
,case when count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end )=0 then 0 else count(case when "fl"='老客' then t1.member_id end )/count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end ) end  as "平均每店老客消费人数"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else count(case when "fl"='老客' then  t2.member_id end )/count(case when "fl"='老客' then t1.member_id end )end "老客年复购率"
,case when count(case when "fl"='老客' then t1.member_id end)=0 then 0 else sum(case when "fl"='老客' then t1.bt end )/count(case when "fl"='老客' then t1.member_id end) end as "老客年消费频次"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else sum(case when "fl"='老客' then  t1.sale_amt end )/count(case when "fl"='老客' then t1.member_id end ) end  as "年产值"
,case when count(c.customer_id)=0 then 0 else  count(case when "fl"='新客' then c.customer_id end )/count( customer_id) end  "新增会员人数占比"
,case when count(case when "fl"='新客' then c.customer_id end )=0 then 0 else count(case when "fl"='新客' and t1.member_id is not null then c.customer_id end )/count(case when "fl"='新客' then c.customer_id end ) end "转化率"
,case when count(case when "fl"='新客' then t1.member_id end)=0 then 0 else sum(case when "fl"='新客' then t1.bt end )/count(case when "fl"='新客' then t1.member_id end) end as "新客年消费频次"
,case when count(case when "fl"='新客' then t1.member_id end )=0 then 0 else sum(case when "fl"='新客' then  t1.sale_amt end )/count(case when "fl"='新客' then t1.member_id end ) end  "新客年产值"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.mb_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店慢病消费人数"
,case when count(case when t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.mb_sale_amt>0 then t2.member_id end )/count(case when t1.mb_sale_amt>0 then t1.member_id end )end  "慢病品类年复购率"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_bt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年消费频次"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_sale_amt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年产"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.bjsp_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店健康品类消费人数"
,case when count(case when t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.bjsp_sale_amt>0 then t2.member_id end )/count(case when t1.bjsp_sale_amt>0 then t1.member_id end )end  "健康品类年复购率"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_bt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年消费频次"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_sale_amt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年产"
from 
:var01 c
inner join :var0 t on c.at_store=t.phmc_code
left join :var_sale_1 t1 on c.customer_id=t1.member_id
left join :var_sale_2 t2 on t1.member_id=t2.member_id
--where c.STATE='EFC' --取有效会员
group by PROP_ATTR
;
var32=
select * 
from :var11 t
left join :var21 t1 on t.PROP_ATTR1=t1.PROP_ATTR 
;
---------按分收购统计


---------按全公司统计
--公司门店统计
var12=
select 
'全公司'PROP_ATTR1
,'合计'adms_org_name1
,count(1)"门店数" --门店数
,count(case when CLOSE_DATE>now() or CLOSE_DATE is null then phmc_code end ) "未关停门店数" --未关停门店数
from :var0 t
where phmc_code=NEW_PHMC_CODE --剔出门店编码变化的门店（旧门店）
;
--公司会员统计

var22=
select 
'全公司'PROP_ATTR
,'合计'adms_org_name
,count(customer_id)all_mt --总会员数
,count(case when "fl"='老客' then customer_id end ) lk_mt --老客数
,case when count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end )=0 then 0 else count(case when "fl"='老客' then t1.member_id end )/count(distinct case when "fl"='老客' and t1.member_id is not null then c.at_store end ) end  as "平均每店老客消费人数"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else count(case when "fl"='老客' then  t2.member_id end )/count(case when "fl"='老客' then t1.member_id end )end "老客年复购率"
,case when count(case when "fl"='老客' then t1.member_id end)=0 then 0 else sum(case when "fl"='老客' then t1.bt end )/count(case when "fl"='老客' then t1.member_id end) end as "老客年消费频次"
,case when count(case when "fl"='老客' then t1.member_id end )=0 then 0 else sum(case when "fl"='老客' then  t1.sale_amt end )/count(case when "fl"='老客' then t1.member_id end ) end  as "年产值"
,case when count(c.customer_id)=0 then 0 else  count(case when "fl"='新客' then c.customer_id end )/count( customer_id) end  "新增会员人数占比"
,case when count(case when "fl"='新客' then c.customer_id end )=0 then 0 else count(case when "fl"='新客' and t1.member_id is not null then c.customer_id end )/count(case when "fl"='新客' then c.customer_id end ) end "转化率"
,case when count(case when "fl"='新客' then t1.member_id end)=0 then 0 else sum(case when "fl"='新客' then t1.bt end )/count(case when "fl"='新客' then t1.member_id end) end as "新客年消费频次"
,case when count(case when "fl"='新客' then t1.member_id end )=0 then 0 else sum(case when "fl"='新客' then  t1.sale_amt end )/count(case when "fl"='新客' then t1.member_id end ) end  "新客年产值"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.mb_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店慢病消费人数"
,case when count(case when t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.mb_sale_amt>0 then t2.member_id end )/count(case when t1.mb_sale_amt>0 then t1.member_id end )end  "慢病品类年复购率"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_bt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年消费频次"
,case when count(case when  t1.mb_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.mb_sale_amt>0 then t1.mb_sale_amt end )/count(case when t1.mb_sale_amt>0 then t1.member_id end ) end as "慢病品类年产"
,case when count(distinct case when t1.member_id is not null then c.at_store end )=0 then 0 else sum(case when t1.bjsp_sale_amt>0 then 1 end)/count(distinct case when t1.member_id is not null then c.at_store end ) end "平均每店健康品类消费人数"
,case when count(case when t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else count(case when t1.bjsp_sale_amt>0 then t2.member_id end )/count(case when t1.bjsp_sale_amt>0 then t1.member_id end )end  "健康品类年复购率"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_bt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年消费频次"
,case when count(case when  t1.bjsp_sale_amt>0 then t1.member_id end )=0 then 0 else sum(case when  t1.bjsp_sale_amt>0 then t1.bjsp_sale_amt end )/count(case when  t1.bjsp_sale_amt>0 then t1.member_id end ) end  "健康品类年产"
from 
:var01 c
inner join :var0 t on c.at_store=t.phmc_code
left join :var_sale_1 t1 on c.customer_id=t1.member_id
left join :var_sale_2 t2 on t1.member_id=t2.member_id
--where c.STATE='EFC' --取有效会员
;
var33=
select * 
from :var12 t
left join :var22 t12 on 1=1 
;

---------按全公司统计

select 
*
from :var31
union select * from :var32
union select * from :var33;

end 
;
