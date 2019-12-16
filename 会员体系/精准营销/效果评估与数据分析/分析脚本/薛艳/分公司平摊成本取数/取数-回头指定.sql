--提取近主流数据
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
  where begin_time >=ADD_MONTHS('20181031',-5)
       and begin_time <add_days(to_date('20181031','yyyymmdd'),1)
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
		UPDATE_DATE_SEND>=ADD_MONTHS('20181031',-5)
		and UPDATE_DATE_SEND<add_days(to_date('20181031','yyyymmdd'),1)
		and PROMOTIONSOURCE='B'
)t 
on z.TEMPLATE_ID||z.customer_id||to_char(z.create_time,'yyyymmdd')=t.CODE||t.member_id||to_char(t.UPDATE_DATE_SEND,'yyyymmdd'));

 ---用于计算发券数；
drop table ext_tmp.xueyan_Y2;
create column table  ext_tmp.xueyan_Y2
as
(select 
to_date('20181031','yyyymmdd') as stsc_date,
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
where begin_time >=ADD_DAYS(LAST_DAY(ADD_MONTHS(to_date('20181031','yyyymmdd'),-1)),1)
       and begin_time <add_days(to_date('20181031','yyyymmdd'),1)
  GROUP BY 
  COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME);--开卡门店行政组织名称

drop table ext_tmp.xueyan_Y3;
create column table  ext_tmp.xueyan_Y3
as
(select
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME ,--开卡门店行政组织名称
 sum(YQ_BULL_COUNT) as YQ_BULL_COUNT,
sum(ht_zd_sale_money) as ht_zd_sale_money,
sum(ht_zd_gross_money) as ht_zd_gross_money
from 
EXT_TMP.BI_COUPON_XGPG_HT_COMPANY 
where
data_date between '2018-10-01' and '2018-10-31'
and ((COUPON_FLAG ='NEW' AND COUPON_LOGICAL_FLAG='NO_CONSUME')
or COUPON_FLAG in('WAKE_UP'))
group by 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
  ADMS_ORG_CODE ,--开卡门店行政组织编码
ADMS_ORG_NAME 
order by COUPON_FLAG);--开卡门店行政组织名称
 
 
 select 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 sum(VCH_QTY),
 sum(YQ_BULL_COUNT),
 sum(ht_zd_sale_money),
 sum(ht_zd_gross_money) 
 from(
 select 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME ,--开卡门店行政组织名称
 VCH_QTY,
 null as YQ_BULL_COUNT,
null as ht_zd_sale_money,
null as ht_zd_gross_money 
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
 YQ_BULL_COUNT,
 ht_zd_sale_money,
 ht_zd_gross_money 
from ext_tmp.xueyan_Y3)
group by 
COUPON_FLAG,
 COUPON_LOGICAL_FLAG,
 ADMS_ORG_CODE ,--开卡门店行政组织编码
 ADMS_ORG_NAME 
 order by 
 COUPON_FLAG,
 COUPON_LOGICAL_FLAG;