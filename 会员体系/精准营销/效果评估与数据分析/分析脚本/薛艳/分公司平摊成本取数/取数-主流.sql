--提取近两个月主流数据
drop table ext_tmp.xueyan_Y1;
create column table  ext_tmp.xueyan_Y1
as
(select 
		z.begin_time,
		z.EXPIRE_TIME, 
		z.used_time,
		z.CUSTOMER_ID,
		z.TEMPLATE_ID,
		z.CREATE_TIME,
		z.coupon_no,
		z.order_code,
		b.coupon_name ,
		b.creator,
		b.coupon_type,
	    t.PROMOTIONSOURCE,
		t.COUPON_FLAG,
		t.COUPON_LOGICAL_FLAG,	
		t.UPDATE_DATE_PROD,
		t.UPDATE_DATE_SEND,
		T.AB_TEST_FLAG
	from (select 
        begin_time,
		EXPIRE_TIME, 
		create_time,
		CUSTOMER_ID,
		TEMPLATE_ID,
		used_time,
		code as coupon_no,
		order_code
 from "DS_CRM"."ZT_COUPON" 
  where begin_time >=ADD_MONTHS('20181130',-2)
       and begin_time <add_days(to_date('20181130','yyyymmdd'),1)
)z
	inner join (
		select 
			coupon_name,
			uuid,
			coupon_type,
			creator
		from  "DS_CRM"."ZT_COUPON_TEMPLATE" 
	)b on b.uuid=z.TEMPLATE_ID
inner join(
	select 
		CODE,
		PROMOTIONSOURCE,
		COUPON_FLAG,
		COUPON_LOGICAL_FLAG,
		COUPONTYPE,	
		AB_TEST_FLAG,	
		UPDATE_DATE_PROD,
		member_id,
		UPDATE_DATE_SEND
	from 
		dm.bi_memb_coupon_combine_hist
	where
		UPDATE_DATE_SEND>=ADD_MONTHS('20181130',-2)
		and UPDATE_DATE_SEND<add_days(to_date('20181130','yyyymmdd'),1)
		and PROMOTIONSOURCE='B'
)t 
on z.TEMPLATE_ID||z.customer_id||to_char(z.create_time,'yyyymmdd')=t.CODE||t.member_id||to_char(t.UPDATE_DATE_SEND,'yyyymmdd'));

drop table ext_tmp.xueyan_Y2;
create column table  ext_tmp.xueyan_Y2
as
(select 
to_date('20181130','yyyymmdd') as stsc_date,
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 COUNT(DISTINCT COUPON_NO) AS VCH_QTY
 --COUNT(DISTINCT CUSTOMER_ID) AS VCH_MEMBER_QTY
 from  ext_tmp.xueyan_Y1 a
 left join (select  
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
where begin_time >=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181130','yyyymmdd'),-1)),1)
       and begin_time <add_days(to_date('20181130','yyyymmdd'),1)
  GROUP BY 
  COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME);--开卡门店行政组织名称
 
 --统计本月销售明细数据
 drop table ext_tmp.xueyan_Y3;
create column table  ext_tmp.xueyan_Y3
as 
(
select *
from
( select 
		begin_time,
		EXPIRE_TIME, 
		used_time,
		CUSTOMER_ID,
		TEMPLATE_ID,
		CREATE_TIME,
		coupon_no,
		order_code,
		coupon_name ,
		creator,
		coupon_type,
	    PROMOTIONSOURCE,
		COUPON_FLAG,
		COUPON_LOGICAL_FLAG,	
		UPDATE_DATE_PROD,
		UPDATE_DATE_SEND,
		s.back_bill_code,
		s.back_member_id,
		s.sale_time,
		s.store_code
	from ext_tmp.xueyan_Y1 z
	inner join (select sale_bill_code  as back_bill_code
						 ,member_id as back_member_id
						 ,sale_time
						 ,store_code
				from ds_pos.sales_order 
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181130','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181130','yyyymmdd'),1)) s --当月销售
				 on s.back_member_id=z.customer_id 
					and sale_time>=z.begin_time and sale_time<=z.EXPIRE_TIME --起关键作用
	union	
	select
		begin_time,
		EXPIRE_TIME, 
		used_time,
		CUSTOMER_ID,
		TEMPLATE_ID,
		CREATE_TIME,
		coupon_no,
		order_code,
		coupon_name ,
		creator,
		coupon_type,
	    PROMOTIONSOURCE,
		COUPON_FLAG,
		COUPON_LOGICAL_FLAG,	
		UPDATE_DATE_PROD,
		UPDATE_DATE_SEND,
		s.back_bill_code,
		s.back_member_id,
		s.sale_time,
		s.store_code
	from ext_tmp.xueyan_Y1 z
	inner join (select sale_bill_code  as back_bill_code
						 ,member_id as back_member_id
						 ,sale_time
						 ,store_code
				from ds_pos.sales_order 
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181130','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181130','yyyymmdd'),1))
				s on z.order_code=s.back_bill_code	
			and sale_time>=z.begin_time and sale_time<=z.EXPIRE_TIME
			
)s);

--关联指定。
drop table  ext_tmp.xueyan_Y4;
create column table  ext_tmp.xueyan_Y4
as 
(
select *
from
( select 
       	begin_time,
		EXPIRE_TIME, 
		used_time,
		CUSTOMER_ID,
		TEMPLATE_ID,
		CREATE_TIME,
		coupon_no,
		order_code,
		coupon_name ,
		creator,
		coupon_type,
	    PROMOTIONSOURCE,
		COUPON_FLAG,
		COUPON_LOGICAL_FLAG,	
		UPDATE_DATE_PROD,
		UPDATE_DATE_SEND,
		back_bill_code,
		back_member_id,
		sale_time,
        store_code
   from ext_tmp.xueyan_Y3) s 
left join(select  UUID, CATEGORY_CODE,good_code,sale_bill_code,sale_money,account_price_gross_money
				from"DS_POS"."SALES_ORDERDETAILS"
				where sale_time>=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181130','yyyymmdd'),-1)),1)
				and sale_time<add_days(to_date('20181130','yyyymmdd'),1)
			  )o on s.back_bill_code =o.sale_bill_code
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
			,code as CATEGORY_CODES
			from "DS_CRM"."ZT_COUPON_TEMPLATE_CATEGORY"
	)t2 on t2.CATEGORY_TEMPLATE_ID=s.TEMPLATE_ID 
	   and left(o.CATEGORY_CODE,length(t2.CATEGORY_CODES))=t2.CATEGORY_CODES);




drop table  ext_tmp.xueyan_Y5;
create column table  ext_tmp.xueyan_Y5
as 
(select 
COUPON_FLAG,
COUPON_LOGICAL_FLAG,	
ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
--count(distinct back_bill_code) as BACK_ORDER_QTY,-- 回头订单数
--count(distinct back_member_id) as BACK_MEMBER_QTY, -- 回头会员数
count(distinct case when order_code is not null and order_code=back_bill_code  then order_code end) as USE_TICK_ORDER_QTY, -- 用券订单数
--count(distinct case when order_code is not null and order_code=back_bill_code  then CUSTOMER_ID end) as USE_TICK_MEMBER_QTY, -- 用券会员数 x
sum(case when rn_1=1 and rn_2=1 and (TEMPLATE_ID=good_TEMPLATE_ID or TEMPLATE_ID=CATEGORY_TEMPLATE_ID or coupon_type='ALL') then sale_money end) as BACK_ASSI_SALE, --回头指定销售额
sum(case when rn_1=1 and  rn_2=1 and (TEMPLATE_ID=good_TEMPLATE_ID or TEMPLATE_ID=CATEGORY_TEMPLATE_ID or coupon_type='ALL') then account_price_gross_money end) as BACK_ASSI_GROS_PROF_AMT --回头指定毛利额
--sum(case when order_code is not null and order_code=back_bill_code  then sale_money end) as USE_TICK_SALE_AMT, -- 用券整单销售额
--sum(case when order_code is not null and order_code=back_bill_code  then ACCOUNT_PRICE_GROSS_MONEY end) as USE_TICK_GROS_PROF_AMT -- 用券整单毛利额 
from 
(select 
*,
row_number() over(partition by customer_id,template_id,uuid order by ORDER_CODE desc, create_time asc) as rn_1,--为了区分同天同人同券模板不同券号（为了明细数据）
row_number() over(partition by customer_id,uuid order by ORDER_CODE desc, create_time asc) as rn_2 
from ext_tmp.xueyan_Y4) a
inner join  "DW"."DIM_PHMC"  d
on a.store_code=d.PHMC_CODE
group by 
COUPON_FLAG,
COUPON_LOGICAL_FLAG,
ADMS_ORG_CODE,
ADMS_ORG_NAME
order by COUPON_FLAG,COUPON_LOGICAL_FLAG);

select 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 sum(VCH_QTY),
 sum(USE_TICK_ORDER_QTY),
 sum(BACK_ASSI_SALE),
 sum(BACK_ASSI_GROS_PROF_AMT) 
 from(
 select 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 VCH_QTY,
 null as USE_TICK_ORDER_QTY,
null as BACK_ASSI_SALE,
null as BACK_ASSI_GROS_PROF_AMT 
 from ext_tmp.xueyan_Y2 
 where ((COUPON_FLAG ='NEW' AND COUPON_LOGICAL_FLAG='NO_CONSUME')
or COUPON_FLAG in('WAKE_UP'))
 union 
 select 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 null as VCH_QTY,
 USE_TICK_ORDER_QTY,
 BACK_ASSI_SALE,
 BACK_ASSI_GROS_PROF_AMT 
from ext_tmp.xueyan_Y5
where ((COUPON_FLAG ='NEW' AND COUPON_LOGICAL_FLAG='NO_CONSUME')
or COUPON_FLAG in('WAKE_UP')))
group by 
COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME 
 order by 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG;




