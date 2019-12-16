 --提取近90天的发券信息（用于寻找在本月回头的券信息）
drop table ext_tmp.xueyan_Y1;
create column table  ext_tmp.xueyan_Y1
as
(select 
 CUSTOMER_ID,
 TEMPLATE_ID,
 begin_time,
EXPIRE_TIME,
create_time,
 coupon_no,
 coupon_type,
 order_code,
 substr_before("NAME",'-') as FST_TOTAL_COUPONTYPE,--券一级类型
substr_before(substr_after("NAME",'-'),'-') as SND_TOTAL_COUPONTYPE --券二级类型
 from
(select 
			uuid,
			coupon_name as name,
			coupon_type,
			'' as coupon_flag,
			'' as promotionsource
    	from  "DS_CRM"."ZT_COUPON_TEMPLATE"  
		where CREATOR in ('00030229','00022125','00103443','00109159','00114101') 
union
select 
 code as uuid,
 '' as name,
 '' as coupon_type,
 coupon_flag,
 promotionsource
from
 "DW"."BI_TEMP_COUPON_ALL" 
 where promotionsource='C') s
 inner join 
 (select 
        begin_time,
		EXPIRE_TIME, 
		create_time,
		CUSTOMER_ID,
		TEMPLATE_ID,
		used_time,
		code as coupon_no,
		order_code
 from "DS_CRM"."ZT_COUPON" 
  where begin_time >=ADD_MONTHS('20181031',-3)
       and begin_time <add_days(to_date('20181031','yyyymmdd'),1)
) z
 on s.uuid=z.TEMPLATE_ID);
 
 --select * from ext_tmp.xueyan_Y1 where CUSTOMER_ID='b179003fc41a4d3a8d7b8f2443873612'
 
 
 ---汇总主题券数据源；用于计算发券数的底层数据(begin_time)；
drop table ext_tmp.xueyan_Y2;
create column table  ext_tmp.xueyan_Y2
as
(select 
 CUSTOMER_ID,
 TEMPLATE_ID,
 coupon_no,
 order_code,
 FST_TOTAL_COUPONTYPE,--券一级类型
 SND_TOTAL_COUPONTYPE, --券二级类型
ADMS_ORG_CODE,
ADMS_ORG_NAME
 from  ext_tmp.xueyan_Y1 a
 inner join (select  
	            memb_code,--会员编码
              MEMB_CARD_CODE,
              MEMB_MOBI,
	            open1_phmc_code ,--开卡门店编码
	            PHMC_F_NAME ,--开卡门店全称
	            ADMS_ORG_CODE ,--开卡门店行政组织编码
                ADMS_ORG_NAME ,--开卡门店行政组织名称
                STRG_MGT_DEPT_CODE,--开卡门店门管部编码
                STRG_MGT_DEPT_NAME,--开卡门店门管部名称
                DIST_CODE,--开卡门店片区名称 
                DIST_NAME --开卡门店片区名称 
	            from "DW"."FACT_MEMBER_BASE"  m
	            inner join 
	            "DW"."DIM_PHMC" d
	            on m.open1_phmc_code=d.PHMC_CODE) t1
        on a.CUSTOMER_ID=t1.memb_code
where begin_time >=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181031','yyyymmdd'),-1)),1)
       and begin_time <add_days(to_date('20181031','yyyymmdd'),1));
       
--统计发券数
drop table  ext_tmp.xueyan_Y1_1;
create column table  ext_tmp.xueyan_Y1_1
as 
(
select 
to_date('20181031','yyyymmdd') as STSC_DATE, --日期   
FST_TOTAL_COUPONTYPE,--券一级类型
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
count(distinct coupon_no ) as SEND_COUP_CNT, --发券数--
count(distinct CUSTOMER_ID) as SEND_COUP_MEMB_CNT --发券会员数 x
from  ext_tmp.xueyan_Y2
group by 
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
FST_TOTAL_COUPONTYPE--券一级类型
);--券二级类型
 
 select * from ext_tmp.xueyan_Y1_1
 
 
 --统计本月销售明细数据
 drop table ext_tmp.xueyan_Y3;
create column table  ext_tmp.xueyan_Y3
as 
(
select *
from
( select 
		z.begin_time,
		z.EXPIRE_TIME, 
		z.create_time,
		z.FST_TOTAL_COUPONTYPE,
		z.SND_TOTAL_COUPONTYPE,
		z.CUSTOMER_ID,
		z.TEMPLATE_ID,
		z.coupon_no,
		z.order_code,
		z.coupon_type,
		s.ht_bill_code,
		s.ht_member_id,
		s.sale_time,
		s.store_code
	from ext_tmp.xueyan_Y1 z
	inner join (select sale_bill_code  as ht_bill_code
						 ,member_id as ht_member_id
						 ,sale_time
						 ,store_code
				from ds_pos.sales_order 
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181031','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181031','yyyymmdd'),1)) s --当月销售
				 on s.ht_member_id=z.customer_id 
					and sale_time>=z.begin_time and sale_time<=z.EXPIRE_TIME --起关键作用
	union	
	select
		z.begin_time,
		z.EXPIRE_TIME, 
		z.create_time,
		z.FST_TOTAL_COUPONTYPE,
		z.SND_TOTAL_COUPONTYPE,
		z.CUSTOMER_ID,
		z.TEMPLATE_ID,
		z.coupon_no,
		z.order_code,
		z.coupon_type,
		s.ht_bill_code,
		s.ht_member_id,
		s.sale_time,
		s.store_code
	from ext_tmp.xueyan_Y1 z
	inner join (select sale_bill_code  as ht_bill_code
						 ,member_id as ht_member_id
						 ,sale_time
						 ,store_code
				from ds_pos.sales_order 
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181031','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181031','yyyymmdd'),1))
				s on z.order_code=s.ht_bill_code	
			and sale_time>=z.begin_time and sale_time<=z.EXPIRE_TIME
			
)s);



--关联销售明细等数据。
drop table  ext_tmp.xueyan_Y4;
create column table  ext_tmp.xueyan_Y4
as 
(
select *
from
( select 
        begin_time,
		EXPIRE_TIME, 
		create_time,
		FST_TOTAL_COUPONTYPE,
		SND_TOTAL_COUPONTYPE,
		CUSTOMER_ID,
		TEMPLATE_ID,
		coupon_no,
		order_code,
		store_code,
		COUPON_TYPE,
		ht_bill_code,
		ht_member_id
   from ext_tmp.xueyan_Y3) s 
left join(select  UUID, CATEGORY_CODE,good_code,sale_bill_code,sale_money,account_price_gross_money,sale_time
				from"DS_POS"."SALES_ORDERDETAILS"
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181031','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181031','yyyymmdd'),1)
			  )o on s.ht_bill_code =o.sale_bill_code			
	left join (
	--券模板指定单品商品
			select 
			TEMPLATE_ID as good_TEMPLATE_ID,
			code as  goods_codes
			from  
			"DS_CRM"."ZT_COUPON_TEMPLATE_SINGLE_GOODS"
	)t1 on t1.good_TEMPLATE_ID=s.TEMPLATE_ID and ( t1.goods_codes=o.good_code)
	left join (
	--券模板指定品类
			select 
			TEMPLATE_ID as CATEGORY_TEMPLATE_ID
			,code as TEMPLATE_ID_code
			from "DS_CRM"."ZT_COUPON_TEMPLATE_CATEGORY"
	)t2 on t2.CATEGORY_TEMPLATE_ID=s.TEMPLATE_ID 
	   and left(o.CATEGORY_CODE,length(t2.TEMPLATE_ID_code))=t2.TEMPLATE_ID_code);	



drop table  ext_tmp.xueyan_Y4_5;
create column table  ext_tmp.xueyan_Y4_5
as 
(
select 
to_date('20181031','yyyymmdd') as STSC_DATE, --日期   
--TEMPLATE_ID,
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
FST_TOTAL_COUPONTYPE,--券一级类型

count(distinct case when order_code is not null and order_code=ht_bill_code  then order_code end) as USE_COUP_ORDER_CNT, -- 用券订单数

sum(case when order_code is not null and order_code=ht_bill_code  then sale_money end) as USE_COUP_SALE_MONEY, -- 用券整单销售额
sum(case when order_code is not null and order_code=ht_bill_code  then ACCOUNT_PRICE_GROSS_MONEY end) as USE_COUP_GROSS_MONEY -- 用券整单毛利额
from  ext_tmp.xueyan_Y4 a
inner join  "DW"."DIM_PHMC"  d
on a.store_code=d.PHMC_CODE
group by 
--TEMPLATE_ID,
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
FST_TOTAL_COUPONTYPE);

select 
stsc_date,
FST_TOTAL_COUPONTYPE,--券一级类型
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
sum(SEND_COUP_CNT), --发券数--
sum(SEND_COUP_MEMB_CNT), --发券会员数 x
sum(USE_COUP_ORDER_CNT),
sum(USE_COUP_SALE_MONEY),
sum(USE_COUP_GROSS_MONEY)
from(select 
stsc_date,
FST_TOTAL_COUPONTYPE,--券一级类型
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
SEND_COUP_CNT, --发券数--
SEND_COUP_MEMB_CNT, --发券会员数 x
null as USE_COUP_ORDER_CNT,
null as USE_COUP_SALE_MONEY,
null as USE_COUP_GROSS_MONEY
from ext_tmp.xueyan_Y1_1
union
select 
stsc_date,
FST_TOTAL_COUPONTYPE,--券一级类型
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
null as SEND_COUP_CNT, --发券数--
null as SEND_COUP_MEMB_CNT, --发券会员数 x
USE_COUP_ORDER_CNT,
USE_COUP_SALE_MONEY,
USE_COUP_GROSS_MONEY
from ext_tmp.xueyan_Y4_5)
group by 
stsc_date,
FST_TOTAL_COUPONTYPE,--券一级类型
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME --开卡门店行政组织名称



