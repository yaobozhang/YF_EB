select
 to_date(begin_time),
 template_id,
 ADMS_ORG_CODE,
 ADMS_ORG_NAME,
 count(distinct code) as VCH_QTY
 from  "DS_CRM"."ZT_COUPON" z
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
        on z.CUSTOMER_ID=t1.memb_code
where template_id='5c00cd06e55eb41f00fd0ed1' 
group by 
to_date(begin_time),
 template_id,
 ADMS_ORG_CODE,
 ADMS_ORG_NAME;
 
-- select top 10 * from "DS_CRM"."ZT_COUPON" 
