--胡娟
--20191126

--首先，拿到湘北公司所有订单
With t1_1 as(
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",								--会员编码
			  t."SALE_ORDR_DOC",  							--销售订单号
			 to_char(t."STSC_DATE",'YYYY') as AT_TEAR,		--年份
			 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--月份
			 --case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 case when  to_char(h.crea_time,'YYYYMMDD')<'20170101' then 'old' 
				when  to_char(h.crea_time,'YYYYMMDD')>='20170101' then 'new'
				else 'no' end as memb_type,					--会员类型
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20190101'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20170101')) t 
		inner JOIN dw.dim_phmc g ON t.PHMC_CODE=g.PHMC_CODE
		LEFT JOIN "DW"."FACT_MEMBER_BASE" h on t.MEMBER_ID=h.MEMB_CODE
		where g.ADMS_ORG_CODE='100A'
		and t.PHMC_CODE!='6710'
		and to_char(g.move_Date,'YYYY')='2016'
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID",									 --会员编码
			 t."SALE_ORDR_DOC",  							--销售订单号
			case when  to_char(h.crea_time,'YYYYMMDD')<'20170101' then 'old' 
				when   to_char(h.crea_time,'YYYYMMDD')>='20170101' then 'new'
				else 'no' end
	)
,

--然后，得到总销售
t1_2 as (
		SELECT AT_TEAR,SUM(SALE_AMT),count(1) as sale_num,count(distinct SALE_ORDR_DOC)
		from t1_1
		group by AT_TEAR

)
select * from t1_2





