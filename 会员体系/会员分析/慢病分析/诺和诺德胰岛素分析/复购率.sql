with t1 as (
			SELECT
				 t."UUID",	   								--明细唯一编码
				 t."STSC_DATE",  								--销售日期
				 case when weekday("STSC_DATE")+1>=1 and weekday("STSC_DATE")+1<=5 then 'Y' ELSE 'N'  END as is_weekday,
				 t."SALE_ORDR_DOC",  							--销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",     							--门店编码
				 t."GOODS_CODE",    							--商品编码
				 t."MEMBER_ID",								--会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.PROD_CATE_LEV3_CODE,
				 g.PROD_CATE_LEV3_NAME,
				 g.GOODS_NAME,
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190829'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20150101')) t 
			left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
			GROUP BY t."UUID",								   --明细唯一编码											   
				 t."STSC_DATE",                                  --销售日期
				 t."SALE_ORDR_DOC",                              --销售订单号
                 t."ORDR_SALE_TIME", 
				 t."PHMC_CODE",                                  --门店编码
				 t."GOODS_CODE",                                 --商品编码
				 t."MEMBER_ID",									 --会员编码
				 g.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
				 g.PROD_CATE_LEV1_NAME,
				 g.PROD_CATE_LEV2_CODE,
				 g.PROD_CATE_LEV2_NAME,
				 g.PROD_CATE_LEV3_CODE,
				 g.PROD_CATE_LEV3_NAME,
				 g.GOODS_NAME
		)
	,                                
	 
			t1_1 as (
			select 
			s.stsc_date,
			s.is_weekday,
			s.AT_YEAR,
			s.member_id,
		    s.is_member,
			case when f.store_code is not null then 'Y' ELSE 'N' END AS IS_NCD_STORE,
			F.NCD_BEGIN_TIME,--慢病项目开启时间
			s.phmc_code,
			s.sale_amt,
			s.GROS_PROF_AMT,
			s.GOODS_CODE,
			s.GOODS_NAME,
			t.phmc_type,
			t.ADMS_ORG_CODE,
			c.birthday,
			c.sex,
			C.create_time,
			c.come_from,
			n.phmc_type_name,
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME,
			S.PROD_CATE_LEV3_CODE,
			S.PROD_CATE_LEV3_NAME,
			CASE WHEN s.PROD_CATE_LEV2_CODE='Y0108' OR S.PROD_CATE_LEV3_CODE='Y010305' THEN 'Y' ELSE 'N' END AS IS_TNB, --是否糖尿病
			CASE WHEN s.GOODS_CODE IN ('1022997','1002770','1002708','1016704','1002781','1002761','1002742','1002772','1021278','1002699','1013716',
                                       '1002702','1002705','1029081','1017909','1002711','1016888','1002764','1002726','1002734','1002723','1002751','1002696') THEN 'Y' ELSE 'N' END AS IS_YDS, --是否胰岛素
			CASE WHEN s.GOODS_CODE IN ('1002708','1017909','1002711','1016704','1002723','1002726','1002734','1002770','1013716') THEN 'Y' ELSE 'N' END AS IS_ND	--是否诺和诺德			
			from t1 s
			inner join 
			(select 
			phmc_code,
			phmc_type,
			CIRCLETYPE,
			ADMS_ORG_CODE,
			to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_TIME
			from dw.dim_phmc
			--where ADMS_ORG_CODE in ('100H','100B','100A')--湘北、长沙、湘南
			) t  
			on s.phmc_code=t.phmc_code
			left join  
			( 
			select 
			customer_id,
			birthday,
			sex,
			create_time,
			to_char(create_time,'yyyy') as create_year,
			come_from
			from ds_crm.tp_cu_customerbase
			) c 
			on s.member_id=c.customer_id
			left join 
			(
			select DICT_NAME as phmc_type_name,dict_code
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0') n
			 on t.phmc_type=n.dict_code
			 left join 
			 (select store_code,begin_time AS NCD_BEGIN_TIME from "DS_COL"."SUCCEZ_CD_STORE") f
			 on s.phmc_code=f.store_code
			where  not exists
			 (
			  select 1 from dw.DIM_PHMC g1
			  where g1.PHMC_CODE = s.PHMC_CODE 
			  and 
			  ( 
			   g1.STAR_BUSI_TIME > add_years('20190828',-3)
			   or 
			   (company_code='4%'
			   or PROP_ATTR in ('Z02','Z07')
			   )
			 )
			)
			)
		
	
	,t1_3 as (
	  select 
	  AT_YEAR,
	  stsc_date,
	  phmc_type_name,
	  phmc_code,
	  case when IS_NCD_STORE='N' THEN 'N'
		     WHEN IS_NCD_STORE='Y' AND NCD_BEGIN_TIME <add_years('20190828',-1) THEN 'Y' END AS IS_NCD_STORE, --是否慢病门店,
	  member_id,
	  is_member,
	  IS_TNB,
	  IS_YDS,
	  IS_ND,
	  sale_amt,
	  GROS_PROF_AMT
	  from t1_1)
	  
	,t1_4 as (  
	select 
	    a.stsc_date,
		a.AT_YEAR,
		a.phmc_type_name,
		a.phmc_code,
		a.IS_NCD_STORE, --是否慢病门店
		a.IS_TNB,
		a.IS_YDS,
		a.IS_ND,
		A.member_id,
		case when b.member_id is not null then 'Y' ELSE 'N' END AS IS_TNB_MEMB,
		case when c.member_id is not null then 'Y' ELSE 'N' END AS IS_YDS_MEMB,
		case when d.member_id is not null then 'Y' ELSE 'N' END AS IS_ND_MEMB,
		a.sale_amt,
		a.GROS_PROF_AMT
	from t1_3 a
	left join 
	(select 
		AT_YEAR,
		member_id,
		phmc_code
	from t1_3
	where is_member='Y' and IS_TNB='Y' 
    group by 
		AT_YEAR,
		member_id,
        phmc_code
	) b
    on a.AT_YEAR=b.AT_YEAR
    and a.member_id=b.member_id
	and a.phmc_code=b.phmc_code
	left join 
	(select 
		AT_YEAR,
		member_id,
		phmc_code
	from t1_3
	where is_member='Y' and IS_YDS='Y' 
    group by 
		AT_YEAR,
		member_id,
        phmc_code
	) c
    on a.AT_YEAR=c.AT_YEAR
    and a.member_id=c.member_id
	and a.phmc_code=c.phmc_code
	left join 
	(select 
		AT_YEAR,
		member_id,
		phmc_code
	from t1_3
	where is_member='Y' and IS_ND='Y' 
    group by 
		AT_YEAR,
		member_id,
        phmc_code
	) d
    on a.AT_YEAR=d.AT_YEAR
    and a.member_id=d.member_id
	and a.phmc_code=d.phmc_code
	WHERE a.IS_NCD_STORE is not null and a.is_member='Y')
	
	
	,t1_5 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					IS_TNB_MEMB,
					IS_YDS_MEMB,
					IS_ND_MEMB,
					max(AT_YEAR) as AT_YEAR,
					max(phmc_type_name) as phmc_type_name,
					MAX(IS_NCD_STORE) AS IS_NCD_STORE,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
                    sum(case when IS_TNB='Y' THEN sale_amt END) AS TNB_sale_amt,
                    sum(case when IS_YDS='Y' THEN sale_amt END) AS YDS_sale_amt,
                    sum(case when IS_ND='Y' THEN sale_amt END) AS ND_sale_amt,
                    sum(case when IS_TNB='N' and IS_TNB_MEMB='Y' THEN sale_amt END) AS TNB_GL_sale_amt,
                    sum(case when IS_TNB='N' AND IS_YDS_MEMB='Y' THEN sale_amt END) AS YDS_GL_sale_amt, --胰岛素关联销售（指购买非糖尿病的商品）
                    sum(case when IS_TNB='N' AND IS_ND_MEMB='Y' THEN sale_amt END) AS ND_GL_sale_amt,
                     sum(case when IS_TNB='N' and IS_TNB_MEMB='Y' THEN GROS_PROF_AMT END) AS TNB_GL_GROS_PROF_AMT,
                    sum(case when IS_TNB='N' AND IS_YDS_MEMB='Y' THEN GROS_PROF_AMT END) AS YDS_GL_GROS_PROF_AMT,
                    sum(case when IS_TNB='N' AND IS_ND_MEMB='Y' THEN GROS_PROF_AMT END) AS ND_GL_GROS_PROF_AMT,
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_4
			group by 
			stsc_date,
			member_id,
			phmc_code,
			IS_TNB_MEMB,
			IS_YDS_MEMB,
			IS_ND_MEMB)
	

     --诺和诺德年度复购率
	,t2 as (
	select 
	a.AT_YEAR,
	a.phmc_type_name,
	sum(case when b.member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
	from 
	(select 
	  AT_YEAR,
	  member_id,
	  phmc_type_name,
	  phmc_code
	  from t1_5
	  where IS_ND_MEMB='Y'
	  group by 
	   AT_YEAR,
	  member_id,
	  phmc_type_name,
	  phmc_code) a
	left join 
	(select 
	  AT_YEAR,
	  member_id,
	  phmc_type_name,
	  phmc_code
	  from t1_5
	  where IS_ND_MEMB='Y'
	   group by AT_YEAR,
	  member_id,
	  phmc_type_name,
	  phmc_code) b
	  on a.AT_YEAR=b.AT_YEAR+1
	  and a.member_id=b.member_id
	  and a.phmc_code=b.phmc_code
	 group by 
	   a.AT_YEAR
	   ,a.phmc_type_name
	   )
	  
	  
	  --计算各年度消费会员总数	  
	,t2_1 as ( 
	    select 
		    AT_YEAR,
			phmc_type_name,
			sum(qty) as total_qty
		from (
	    select 
		   AT_YEAR,
			phmc_type_name,
			phmc_code,
			count(*) as qty
		from (select 
			AT_YEAR,
			phmc_type_name,
			phmc_code,
			member_id
        from t1_5
		where IS_ND_MEMB='Y'
		group by AT_YEAR,
			phmc_type_name,
			phmc_code,
			member_id)
		group by AT_YEAR,
			phmc_type_name,
			phmc_code)
	    group by AT_YEAR,
			phmc_type_name)
			
	,t2_2 as (
	    select 
		   a.AT_YEAR,
		   a.phmc_type_name,
	       b.return_memb_num,
	       a.total_qty
	    from t2_1 a
		left join t2  b
		on a.AT_YEAR=b.AT_YEAR
		and a.phmc_type_name=b.phmc_type_name
		)
		
	 select 
	 a.AT_YEAR,
	 a.phmc_type_name,
	 a.return_memb_num,
	 b.total_qty,
	 a.return_memb_num/b.total_qty
	 from 
	 t2_2 a
	 left join t2_2 b
	 on a.AT_YEAR=b.AT_YEAR+1
	 AND a.phmc_type_name=b.phmc_type_name;
		
		
    