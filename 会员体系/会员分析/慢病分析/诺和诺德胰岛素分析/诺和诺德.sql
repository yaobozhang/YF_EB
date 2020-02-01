--诺和诺德分析（品牌赋能）
--代码贡献者：姚泊彰
--代码更新时间：20191231
--数据口径：见各自模块

--STEP1:得到数据源
--STEP1.0:取订单明细数据（该视图的订单数据已做订单基础过滤）

with t1_0 as (
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
				 '20200101'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20190101')) t 
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
	 
--STEP1.1:  关联门店、会员等表，打标
			t1_1 as (
			select 
				s.stsc_date,
				s.is_weekday,
				s.AT_YEAR,				--销售年份
				s.member_id,			--会员编码
				s.is_member,			--是否会员
				case when f.store_code is not null then 'Y' ELSE 'N' END AS IS_NCD_STORE,			--是否慢病门店
				F.NCD_BEGIN_TIME,--慢病项目开启时间
				s.phmc_code,
				s.sale_amt,
				s.GROS_PROF_AMT,
				s.GOODS_CODE,
				s.GOODS_NAME,
				t.phmc_type,
				t.ADMS_ORG_CODE,
				c.ADMS_ORG_NAME,			--分公司名称
				c.birthday,					--生日
				c.sex,						--性别
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
				CASE WHEN s.GOODS_CODE IN (
										'1016704'			--诺和灵N笔
										,'1014029'			--诺和力
										,'1002711'			--诺和灵50R
										,'1011241'			--诺和锐30特充
										,'1016828'			--诺和锐30笔芯
										,'1017909'			--诺和灵预混30R
										,'1020759'			--诺和锐50笔芯
										,'1002723'			--诺和灵R笔
										,'1002770'			--诺和平特充
										,'1013716'			--诺和平笔芯
										,'1002726'			--诺和锐笔芯
										,'1002734'			--诺和锐特充
										) THEN 'Y' ELSE 'N' END AS IS_ND	--是否诺和诺德			
			from t1_0 s
			inner join 
			(	
				select 
					phmc_code,
					phmc_type,
					CIRCLETYPE,
					ADMS_ORG_CODE,
					ADMS_ORG_NAME,
					to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_TIME
				from dw.dim_phmc
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
					come_from,
					t2.ADMS_ORG_NAME
				from ds_crm.tp_cu_customerbase t1
				left join dw.dim_phmc t2
				on t1.at_store=t2.phmc_code
			) c 
			on s.member_id=c.customer_id
			left join 
			(
				select DICT_NAME as phmc_type_name,dict_code
				from "DS_POS"."SYS_DICT"
				where type_code='m_shopType' and deletelable='0'
			) n
			on t.phmc_type=n.dict_code
			left join 
				(
					select store_code,begin_time AS NCD_BEGIN_TIME from "DS_COL"."SUCCEZ_CD_STORE"
				) f
			on s.phmc_code=f.store_code
			where  not exists
			 (
			  select 1 from dw.DIM_PHMC g1
			  where g1.PHMC_CODE = s.PHMC_CODE 
			  and 
			  ( 
			   g1.STAR_BUSI_TIME > add_years('20191231',-3)
			   or 
			   PROP_ATTR in ('Z02','Z03','Z04','Z07')
	
			 )
			)
			)
--STEP1.2: 打标“是否糖尿病会员”、“是否胰岛素会员”、“是否诺和诺德会员”	
,
	t1_2 as (  
				select member_id
					,TNB_YEAR		--最早购买糖尿年份
					,YDS_YEAR		--最早购买胰岛素年份
					,ND_YEAR		--最早购买诺和诺德年份
					,case when IS_TNB>0 then 'Y' else 'N' end as IS_TNB_MEMB
					,case when IS_YDS>0 then 'Y' else 'N' end as IS_YDS_MEMB
					,case when IS_ND>0 then 'Y' else 'N' end as IS_ND_MEMB
				from
				(
					select member_id
						,min(TNB_YEAR) as TNB_YEAR
						,min(YDS_YEAR) as YDS_YEAR
						,min(ND_YEAR) as ND_YEAR
						,sum(IS_TNB) as IS_TNB
						,sum(IS_YDS) as IS_YDS
						,sum(IS_ND) as IS_ND
					from
					(
						select member_id
							,CASE WHEN IS_TNB='Y' then AT_YEAR END AS TNB_YEAR		--最早购买糖尿年份
							,CASE WHEN IS_YDS='Y' then AT_YEAR END AS YDS_YEAR		--最早购买胰岛素年份
							,CASE WHEN IS_ND='Y' then AT_YEAR END AS ND_YEAR		--最早购买诺和诺德年份
							,case when IS_TNB='Y' then 1 else 0 end as IS_TNB
							,case when IS_YDS='Y' then 1 else 0 end as IS_YDS
							,case when IS_ND='Y' then 1 else 0 end as IS_ND
						from t1_1
					)
					group by member_id
					
				)
			)

--STEP1.4: 给每个订单打标哪种会员购买	  
	,
	t1_4 as (  
	select 
	    a.stsc_date,
		a.AT_YEAR,
		a.phmc_type_name,
		a.phmc_code,
		a.IS_NCD_STORE, 
		a.IS_TNB, 
		a.IS_YDS, 
		a.IS_ND, 
		a.member_id
		,a.ADMS_ORG_NAME			--开卡分公司名称
		,floor(days_between(a.birthday,now())/365) as age		--年龄
		,a.sex					--性别
		,b.TNB_YEAR		--最早购买糖尿年份
		,b.YDS_YEAR		--最早购买胰岛素年份
		,b.ND_YEAR		--最早购买诺和诺德年份
		,b.IS_TNB_MEMB
		,b.IS_YDS_MEMB
		,b.IS_ND_MEMB
		,a.sale_amt
		,a.GROS_PROF_AMT
	from t1_1 a
	left join 
	(
		select member_id
			,TNB_YEAR		--最早购买糖尿年份
			,YDS_YEAR		--最早购买胰岛素年份
			,ND_YEAR		--最早购买诺和诺德年份
			,IS_TNB_MEMB
			,IS_YDS_MEMB
			,IS_ND_MEMB
		from t1_2
		
	) b
    on a.member_id=b.member_id
	WHERE a.IS_NCD_STORE is not null and a.is_member='Y'
	)
	
--step1.5：同人同天同门店处理	
	,t1_5 as (
			select
					stsc_date
					,member_id
					,phmc_code
					,max(ADMS_ORG_NAME) as 	ADMS_ORG_NAME		--开卡分公司名称
					,max(age) as age	--年龄
					,max(sex) as sex					--性别
					,max(TNB_YEAR) as TNB_YEAR		--最早购买糖尿年份
					,max(YDS_YEAR) as YDS_YEAR		--最早购买胰岛素年份
					,max(ND_YEAR) as ND_YEAR		--最早购买诺和诺德年份
					,max(IS_TNB_MEMB) as IS_TNB_MEMB			--是否糖尿病会员
					,max(IS_YDS_MEMB) as IS_YDS_MEMB		--是否胰岛素会员
					,max(IS_ND_MEMB) as IS_ND_MEMB,			--是否诺和诺德会员
					max(AT_YEAR) as AT_YEAR,				--年份
					max(phmc_type_name) as phmc_type_name,
					MAX(IS_NCD_STORE) AS IS_NCD_STORE,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
                    sum(case when IS_TNB='Y' THEN sale_amt END) AS TNB_sale_amt,
                    sum(case when IS_YDS='Y' THEN sale_amt END) AS YDS_sale_amt,
                    sum(case when IS_ND='Y' THEN sale_amt END) AS ND_sale_amt,
                    sum(case when IS_TNB='N' and IS_TNB_MEMB='Y' AND TNB_YEAR<=AT_YEAR THEN sale_amt END) AS TNB_GL_sale_amt,
                    sum(case when IS_TNB='N' AND IS_YDS_MEMB='Y' AND YDS_YEAR<=AT_YEAR THEN sale_amt END) AS YDS_GL_sale_amt, --胰岛素关联销售（指购买非糖尿病的商品）
                    sum(case when IS_TNB='N' AND IS_ND_MEMB='Y' AND ND_YEAR<=AT_YEAR THEN sale_amt END) AS ND_GL_sale_amt,
                     sum(case when IS_TNB='N' and IS_TNB_MEMB='Y' AND TNB_YEAR<=AT_YEAR THEN GROS_PROF_AMT END) AS TNB_GL_GROS_PROF_AMT,
                    sum(case when IS_TNB='N' AND IS_YDS_MEMB='Y' AND YDS_YEAR<=AT_YEAR THEN GROS_PROF_AMT END) AS YDS_GL_GROS_PROF_AMT,
                    sum(case when IS_TNB='N' AND IS_ND_MEMB='Y' AND ND_YEAR<=AT_YEAR THEN GROS_PROF_AMT END) AS ND_GL_GROS_PROF_AMT,
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_4
			group by 
			stsc_date,
			member_id,
			phmc_code
		)

  --STEP2:计算相关指标
  ,
  t2 as (
		SELECT 
		AT_YEAR,
		phmc_type_name,
		count(*) AS PHMC_QTY,   --门店数
		SUM(total_QTY) AS total_QTY, --总会员数
		sum(TNB_QTY) AS TNB_QTY, --糖尿病会员数
		sum(YDS_QTY) AS YDS_QTY, --胰岛素会员数
		sum(ND_QTY) AS ND_QTY,  --诺和诺德会员数
		SUM(sale_amt) AS sale_amt, --总销售额
		SUM(TNB_sale_amt) AS TNB_sale_amt, --糖尿病销售额
		SUM(YDS_sale_amt) AS YDS_sale_amt, --胰岛素销售额
		SUM(ND_sale_amt) AS ND_sale_amt, --诺和诺德销售额
		sum(sale_times) as sale_times,  --总消费次数
		sum(TNB_sale_times) AS TNB_sale_times, --糖尿病消费次数
		sum(YDS_sale_times) AS YDS_sale_times, --胰岛素消费次数
		sum(ND_sale_times) AS ND_sale_times,--诺和诺德消费次数
		SUM(TNB_GL_sale_amt)/sum(TNB_QTY) AS TNB_GL_sale_amt,  --糖尿病关联销售
		SUM(YDS_GL_sale_amt)/sum(YDS_QTY) AS YDS_GL_sale_amt, --胰岛素关联销售
		SUM(ND_GL_sale_amt)/sum(ND_QTY) AS ND_GL_sale_amt,--诺和诺德关联销售
		SUM(TNB_GL_GROS_PROF_AMT)/sum(TNB_QTY) AS TNB_GL_GROS_PROF_AMT,--糖尿病关联毛利
		SUM(YDS_GL_GROS_PROF_AMT)/sum(YDS_QTY) AS YDS_GL_GROS_PROF_AMT, --胰岛素关联毛利
		SUM(ND_GL_GROS_PROF_AMT)/sum(ND_QTY) AS ND_GL_GROS_PROF_AMT--诺和诺德关联毛利
		FROM 
		(
			SELECT
				AT_YEAR,
				phmc_type_name,
				phmc_code,
				COUNT(DISTINCT CASE WHEN IS_TNB_MEMB='Y' AND TNB_YEAR<=AT_YEAR THEN MEMBER_ID end) AS TNB_QTY,
				COUNT(DISTINCT CASE WHEN IS_YDS_MEMB='Y' AND YDS_YEAR<=AT_YEAR THEN MEMBER_ID end) AS YDS_QTY,
				COUNT(DISTINCT CASE WHEN IS_ND_MEMB='Y' AND ND_YEAR<=AT_YEAR THEN MEMBER_ID end) AS ND_QTY,
				COUNT(DISTINCT member_id) AS total_QTY,
				sum(sale_times) as sale_times,
				sum(CASE WHEN IS_TNB_MEMB='Y' AND TNB_YEAR<=AT_YEAR THEN sale_times end) AS TNB_sale_times,
				sum(CASE WHEN IS_YDS_MEMB='Y' AND YDS_YEAR<=AT_YEAR THEN sale_times end) AS YDS_sale_times,
				sum(CASE WHEN IS_ND_MEMB='Y' AND ND_YEAR<=AT_YEAR THEN sale_times end) AS ND_sale_times,
				SUM(sale_amt) AS sale_amt,
				SUM(TNB_sale_amt) AS TNB_sale_amt,
				SUM(YDS_sale_amt) AS YDS_sale_amt,
				SUM(ND_sale_amt) AS ND_sale_amt,
				SUM(TNB_GL_sale_amt) AS TNB_GL_sale_amt,
				SUM(YDS_GL_sale_amt) AS YDS_GL_sale_amt,
				SUM(ND_GL_sale_amt) AS ND_GL_sale_amt,
				SUM(TNB_GL_GROS_PROF_AMT) AS TNB_GL_GROS_PROF_AMT,
				SUM(YDS_GL_GROS_PROF_AMT) AS YDS_GL_GROS_PROF_AMT,
				SUM(ND_GL_GROS_PROF_AMT) AS ND_GL_GROS_PROF_AMT
			FROM t1_5 t1
			WHERE IS_NCD_STORE='Y'  --此处'Y'表示是慢病门店，‘N’表示非慢病门店
			GROUP BY AT_YEAR,phmc_type_name,
			phmc_code
	   )
	   group by AT_YEAR,phmc_type_name
   )
   ,
   --STEP3:查看会员分布
   --首先，得到
   t3_0 as (
		select member_id
			,stsc_date
			,phmc_code
			,ADMS_ORG_NAME			--分公司名称
			,age		--年龄
			,sex					--性别
			,sale_amt				--总销售
			,sale_times				--总购买次数
			,ND_sale_amt			--诺和诺德销售
			,case when IS_ND_MEMB='Y' and ND_YEAR<=AT_YEAR THEN sale_times else 0 end AS ND_sale_times	--诺和诺德购买次数
		from t1_5
		where IS_NCD_STORE='Y'			--看慢病门店
			and IS_ND_MEMB='Y'			--拿到诺和诺德会员
			and AT_YEAR='2019'
		
   )
   ,
   t3_1 as (
		select member_id
			,max(ADMS_ORG_NAME) as ADMS_ORG_NAME
			,max(floor((age-15)/5)*5) as age
			,max(sex) as sex
			,sum(sale_amt) as sale_amt
			,sum(sale_times) as sale_times
			,sum(ND_sale_amt) as ND_sale_amt	--诺和诺德销售
			,sum(ND_sale_times) as ND_sale_times	--诺和诺德购买次数
		from t3_0	
		group by member_id
   )
   ,
   t3_2 as (
		SELECT age
			,count(1) as memb_num
			,avg(sale_amt) as sale_amt
			,avg(sale_times) as sale_times
			,avg(ND_sale_amt) as ND_sale_amt
			,avg(ND_sale_times) as ND_sale_times
		FROM T3_1
		where age>=20 and age<=85
		GROUP BY AGE
   )
   ,
   t3_3 as (
		SELECT SEX
			,count(1) as memb_num
			,avg(sale_amt) as sale_amt
			,avg(sale_times) as sale_times
			,avg(ND_sale_amt) as ND_sale_amt
			,avg(ND_sale_times) as ND_sale_times
		FROM T3_1
		GROUP BY SEX
   )
   ,
   t3_4 as (
		SELECT ADMS_ORG_NAME
			,count(1) as memb_num
			,avg(sale_amt) as sale_amt
			,avg(sale_times) as sale_times
			,avg(ND_sale_amt) as ND_sale_amt
			,avg(ND_sale_times) as ND_sale_times
		FROM T3_1
		GROUP BY ADMS_ORG_NAME
   )
   
   select * from t3_4

	




