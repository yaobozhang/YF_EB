--岳阳会员分析
--代码贡献者：薛艳
--代码更新时间：20190822
--数据口径：见各自模块

--简介：会员总体分析总共分为3块：1、门店分析；2、大数；3、会员到年；4、会员画像；5、会员权益；6、品类；

--0、数据准备
    
	--0.1、会员现状口径订单数据：订单数据时间各模块自行调整 ；订单数据基础过滤
	
	/*
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
				 g.GOODS_NAME,
				 to_char(t."STSC_DATE",'YYYY') as AT_YEAR,		--年份
                 to_char(t."STSC_DATE",'YYYYMM') as AT_MONTH,		--年份
				 case when t.member_id is not null then t.member_id else t.sale_ordr_doc end as member_id_final,   --最终会员编码（非会员以订单号为编码）
				 case when t.member_id is not null then 'Y' else 'N' end as is_member,        				 --是否是会员
				 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
				 sum(t."GOODS_SID") AS "GOODS_SID",			--商品编码关联商品表唯一编码
				 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
				 sum("SALE_AMT") AS "SALE_AMT"				--销售额
                 ,sum( case when g.PURC_CLAS_LEV1_CODE='01' then sale_amt end) as PURC_MONEY 	--营销销售
		         ,sum( case when g.PURC_CLAS_LEV1_CODE='02' then sale_amt end) as NO_PURC_MONEY	--非营销
			FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20190816'),
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
				 g.GOODS_NAME
		)
     
	 ,t1_1 as (
			select 
			s.stsc_date,
			s.is_weekday,
			s.AT_YEAR,
			S.AT_MONTH,
			s.member_id_final,
			s.IS_MEMBER,
			case when to_char(s.ORDR_SALE_TIME,'hh24')>='06' and to_char(s.ORDR_SALE_TIME,'hh24')<='07' then '6:00-7:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='08' and to_char(s.ORDR_SALE_TIME,'hh24')<='11' then '8:00-11:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='12' and to_char(s.ORDR_SALE_TIME,'hh24')<='13' then '12:00-13:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='14' and to_char(s.ORDR_SALE_TIME,'hh24')<='17' then '14:00-17:59'
				 when to_char(s.ORDR_SALE_TIME,'hh24')>='18' and to_char(s.ORDR_SALE_TIME,'hh24')<='20' then '18:00-20:59'
				 when (to_char(s.ORDR_SALE_TIME,'hh24')>='21' or to_char(s.ORDR_SALE_TIME,'hh24')<='05') then '21:00-5:59' end as sale_hour,
			s.member_id,
			t.IS_SALE_YY,  --是否消费门店在岳阳
			t.SALE_PHMC_FLAG,
			T.STAR_BUSI_TIME,
			s.phmc_code,
			s.sale_amt,
			s.GROS_PROF_AMT,
			s.GOODS_CODE,
			s.GOODS_NAME,
			c.IS_YY,
			c.OPEN_PHMC_FLAG,
			t.phmc_type,
			c.birthday,
			c.sex,
			C.create_time,
			c.create_year,
			c.come_from,
			n.phmc_type_name,
			s.PROD_CATE_LEV1_CODE,						--品类分析专用，不用时注释
			s.PROD_CATE_LEV1_NAME,
			s.PROD_CATE_LEV2_CODE,
			s.PROD_CATE_LEV2_NAME
			from t1 s
			inner join 
			(select 
			a.phmc_code,
			a.phmc_type,
			CIRCLETYPE,
			to_char(STAR_BUSI_TIME,'yyyymm') as STAR_BUSI_TIME,
			case when b.phmc_code is not null then 'Y' ELSE 'N' END AS IS_SALE_YY,
			b.phmc_type as SALE_PHMC_FLAG
			from dw.dim_phmc a
			left join "EXT_TMP"."YUEYANG_STORE" b
			on a.phmc_code=b.phmc_code) t
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
			case when d.phmc_code is not null then 'Y' ELSE 'N' END AS IS_YY,    --是否开卡门店在岳阳
			d.phmc_type as OPEN_PHMC_FLAG  
			from ds_crm.tp_cu_customerbase c
			left join "EXT_TMP"."YUEYANG_STORE" d
			on c.store=d.phmc_code
			) c 
			on s.member_id=c.customer_id
			left join 
			(
			select DICT_NAME as phmc_type_name,dict_code
			from "DS_POS"."SYS_DICT"
			 where type_code='m_shopType' and deletelable='0') n
			 on t.phmc_type=n.dict_code
			 left join 
			(
			select DICT_NAME as CIRCLETYPE_name,dict_code
			from "DS_POS"."SYS_DICT"
			 where type_code='m_circletype' and deletelable='0') f
			 on t.CIRCLETYPE=f.dict_code
			)
    
	*/
	--1.1  门店分析
	/*
	*/
    --1.2  大数(20140101-20190815)
	/*,t1_2 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					max(AT_YEAR) AS  AT_YEAR,
					max(AT_MONTH) AS AT_MONTH,
					max(create_time) as create_time,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where is_member='Y' and IS_SALE_YY='Y' AND IS_YY='Y' AND SALE_PHMC_FLAG='自营' and OPEN_PHMC_FLAG='自营'   --此处调整是否岳阳开卡、岳阳消费；是否自营、是否加盟
			group by 
			stsc_date,
			member_id,
			phmc_code)
			
	--总消费会员数		
	,t2 as (
       select 
		   count(*)
        from (select 
		   member_id
	   from t1_2
		group  by
		   member_id)
		 )
	--总开卡会员数
		 select 
			count(*)
			from ds_crm.tp_cu_customerbase a
			inner join "EXT_TMP"."YUEYANG_STORE"  b
			on a.store=b.phmc_code
			where phmc_type='自营'
            and create_time<'20190816'
	*/
	--1.3  会员到年（20150101-20190815）
   /*--1.3.1 各销售会员组成类型的人均产值与频次
	,t1_2 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					max(AT_YEAR) AS  AT_YEAR,
					max(AT_MONTH) AS AT_MONTH,
					max(create_year) as create_year,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where is_member='Y' and IS_SALE_YY='Y' AND IS_YY='Y' AND SALE_PHMC_FLAG='自营' and OPEN_PHMC_FLAG='自营' 
			group by 
			stsc_date,
			member_id,
			phmc_code)

	,t1_3 as 
	(select
			AT_YEAR,
			member_id,
			max(create_year) as create_year,
			sum(sale_amt) as sale_amt,
			sum(sale_times) as sale_times
			from t1_2
			group by 
			AT_YEAR,
			member_id
	 )
		
	,  t2 as (	 
		select
		a.AT_YEAR,
		a.member_id,
		case when a.create_year=a.AT_YEAR then '01'   --是否新增
			 when b.member_id is not null then '02'   --是否复购
			 when c.member_id is NOT null and a.create_year<a.AT_YEAR then '03' ----是否历史无消费回归
			 else '04' end as memb_flag,               
		sum(a.sale_amt) as sale_amt,
		sum(a.sale_times) as sale_times
		from t1_3 a
		left join t1_3 b
		on a.AT_YEAR=b.AT_YEAR+1
		and a.member_id=b.member_id 
		left join 
		(select data_date,member_id
		from DM.FACT_MEMBER_CNT_INFO 
		where R_ALL_SONSU_TIMES=0) c
		on c.data_date=add_months(last_day(a.AT_YEAR),-1) 
		and a.member_id=c.member_id
		group by a.AT_YEAR,
				 a.member_id,
			   case when a.create_year=a.AT_YEAR then '01'   --是否新增
			 when b.member_id is not null then '02'   --是否复购
			 when c.member_id is NOT null and a.create_year<a.AT_YEAR then '03' ----是否历史无消费回归
			 else '04' end)
	
    --各类型明细	
	 select 
		AT_YEAR,
		memb_flag,
		count(*) as new_memb_qty, --消费会员数
		 sum(sale_amt)/count(*) as new_memb_avg_sale, --人均年产值
		 sum(sale_times)/count(*) as new_memb_avg_times --人均消费频次 
		from t2 
		group by  AT_YEAR,
		memb_flag
		order by 
		AT_YEAR,
		memb_flag;
	
	--汇总03,04类型
	 select 
    AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end as memb_flag_total,
    count(*) as new_memb_qty, --消费会员数
	 sum(sale_amt)/count(*) as new_memb_avg_sale, --人均年产值
	 sum(sale_times)/count(*) as new_memb_avg_times --人均消费频次 
    from t2 
    group by  AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end
    order by 
    AT_YEAR,
    case when memb_flag in ('01','02') then memb_flag else '其他' end;
	*/	
   /*--1.3.2 岳阳新增会员*/
    /*
	select 
    create_year,
    count(*)
    from (
      select 
			customer_id,
			birthday,
			sex,
			create_time,
			to_char(create_time,'yyyy') as create_year,
			come_from
			from ds_crm.tp_cu_customerbase a
			inner join "EXT_TMP"."YUEYANG_STORE"  b
			on a.store=b.phmc_code
			where phmc_type='自营'
			)
	group by create_year
	order by create_year
	*/
   /*--1.1.3 会员销售占比，会员订单占比(会员的订单是同人同天同门店算一次)*/
  /*
  ,t6 as (
   SELECT 
      AT_YEAR,
      SUM(memb_sale_amt), --会员销售
	  SUM(nomemb_sale_amt), --非会员销售
	  SUM(memb_sale_times), --会员订单
	  SUM(nomemb_sale_times) --非会员订单
   FROM 
   (
    select 
	AT_YEAR,
	case when is_member='Y' then sale_amt end as memb_sale_amt,
	case when is_member='N' then sale_amt end as nomemb_sale_amt,
	case when is_member='Y' then sale_times end as memb_sale_times,
	case when is_member='N' then sale_times end as nomemb_sale_times
	FROM (
    select
					stsc_date,
					member_id_final,
					is_member,
					phmc_code,
					AT_YEAR,
					sum(sale_amt) as sale_amt,          --销售额
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where IS_SALE_YY='Y'and SALE_PHMC_FLAG='自营' 
			group by 
			stsc_date,
			member_id_final,
			is_member,
			phmc_code,
			AT_YEAR
		)
	 )
    group by AT_YEAR
    )
    
    select * from t6;
	*/
	--1.4 会员画像(20180814-20190815)
	/*
	 
	*/
	/*--1.5 会员权益*/--(20180814-20190815)(在岳阳开卡)
	/* 
	--分级：在岳阳开卡（需把消费地点限制放开）
    
	,t1_2 as (
			select
					stsc_date,
					member_id,
					phmc_code,
					max(AT_YEAR) AS  AT_YEAR,
					max(AT_MONTH) AS AT_MONTH,
					max(create_year) as create_year,
					sum(sale_amt) as sale_amt,          --销售额
					sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
					count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
					1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where IS_MEMBER='Y' AND IS_YY='Y' and OPEN_PHMC_FLAG='自营' 
			group by 
			stsc_date,
			member_id,
			phmc_code)


	,t1_3 as
				(
					select member_id,
						  sum(sale_amt) as SALE_AMOUNT,
						  sum(GROS_PROF_AMT) as GROS_PROF_AMT
					from t1_2
					group by member_id
					having sum(sale_amt)>0)
					,
					
   t1_4 as
			(
				select s.member_id
					,SALE_AMOUNT		--每单消费金额
					,GROS_PROF_AMT
					,case when SALE_AMOUNT<100 then 1
					when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
					when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
					when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
					when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
					when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
					when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
					when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
					when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
					when SALE_AMOUNT>=4600 THEN 10 end as LV 
				from t1_3 s
			)
			
			SELECT LV
			    ,count(distinct member_id)
				,sum(GROS_PROF_AMT)
				,sum(SALE_AMOUNT)
			FROM t1_4 
			GROUP BY LV
			
	---生命周期
	select MEMB_LIFE_CYCLE,COUNT(*)
			 from "DM"."FACT_MEMBER_CNT_INFO_V3" a
			inner join  
						( 
						select 
						customer_id,
						birthday,
						sex,
						create_time,
						to_char(create_time,'yyyy') as create_year,
						come_from
						from ds_crm.tp_cu_customerbase a
						inner join "EXT_TMP"."YUEYANG_STORE"  b
						on a.store=b.phmc_code
						where phmc_type='自营'
						) c 
			on a.member_id=c.customer_id
			where data_date='20190815'
			GROUP BY MEMB_LIFE_CYCLE
			order by MEMB_LIFE_CYCLE
	*/
	/*--1.6 品类
	/*
	--品类专用
	,t2_1 as (
		--得到拼接后的品类
		select member_id
		      ,birthday
             ,sex
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,PROD_CATE_LEV1_NAME
			,sum(SALE_AMT) as SALE_AMT
			,count(1) as sale_times
			,sum(GROS_PROF_AMT) as GROS_PROF_AMT
		from
		(
			select	stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
				PROD_CATE_LEV2_NAME,
				PROD_CATE_LEV1_NAME,
				 birthday,
                 sex,
				sum(SALE_AMT) as SALE_AMT,
				sum(GROS_PROF_AMT) as GROS_PROF_AMT
			from
			(
				select stsc_date,					--日期
					member_id,					--会员ID
					phmc_code,					--门店号
					AT_YEAR,					--年份带出来
                    birthday,
                    sex,
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
					  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
					  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV2_NAME,
				CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
	 	             WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
	 	             ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END PROD_CATE_LEV1_NAME	  
				,SALE_AMT
				,GROS_PROF_AMT
				from t1_1 s
				left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
				on S.GOODS_CODE = T.GOODS_CODE
				where is_member='Y' and IS_SALE_YY='Y' AND IS_YY='Y' AND SALE_PHMC_FLAG='自营' and OPEN_PHMC_FLAG='自营' 
			)
			group by stsc_date,					--日期
				member_id,					--会员ID
				phmc_code,					--门店号
				AT_YEAR,					--年份带出来
                birthday,
                sex,
				PROD_CATE_LEV2_NAME,
				PROD_CATE_LEV1_NAME
		)
		group by member_id
			,AT_YEAR
			,PROD_CATE_LEV2_NAME
			,PROD_CATE_LEV1_NAME
			,birthday
            ,sex
	)   
   */
   /*--1.6.1 品类复购率（20150101-20190815)*/
   /*
   ,t4_3 as (
		select	AT_YEAR				--年份
			,PROD_CATE_LEV2_NAME
			,sum(case when member_id is not null then 1 else 0 end) as return_memb_num	--复购人数
			,sum(case when member_id is not null then SALE_AMT else 0 end) as return_memb_sale	--复购金额
			,sum(case when member_id is not null then sale_times else 0 end) as return_memb_times	--复购金额
		from
		(
			select  
				 t1.AT_YEAR,				--年份
				 t1.PROD_CATE_LEV2_NAME,
				 t2.member_id ,				--上一年是否购买
				 t1.SALE_AMT	,			--销售额
				 t1.sale_times				--消费次数
			from t2_1 t1
			left join t2_1 t2
			on t1.AT_YEAR=t2.AT_YEAR+1
			and t1.member_id=t2.member_id
			and t1.PROD_CATE_LEV2_NAME=t2.PROD_CATE_LEV2_NAME
		)
		group by AT_YEAR
		,PROD_CATE_LEV2_NAME
	)
	,
	--计算各年度消费会员总数	  
	t5_3 as ( 
		select 
			AT_YEAR,
			PROD_CATE_LEV2_NAME,
			count(1) as total_qty --消费会员总数	
		from t2_1
		group by AT_YEAR,
			PROD_CATE_LEV2_NAME
	 )
	 ,
	 --得到各年度复购会员数、消费会员数、复购人均消费额、复购人均消费频次
	 t6_3 as 
	 (
		select 	 
			t5.AT_YEAR,					--年份
			T5.PROD_CATE_LEV2_NAME,
			t5.total_qty,				--总消费人数
			t4.return_memb_num,			--复购人数	
			t4.return_memb_sale, 		--总销售额
			t4.return_memb_times 		--总消费次数
		from t5_3 t5
		left join t4_3 t4
		on t5.AT_YEAR=t4.AT_YEAR
		AND T5.PROD_CATE_LEV2_NAME=T4.PROD_CATE_LEV2_NAME
	 )
	 
	 select 
	 a.AT_YEAR,
	 a.PROD_CATE_LEV2_NAME,
	 a.return_memb_num/b.total_qty --品类年度复购率
	 from 
	 t6_3 a
	 left join t6_3 b
	 on a.AT_YEAR=b.AT_YEAR+1
	 AND a.PROD_CATE_LEV2_NAME=b.PROD_CATE_LEV2_NAME  
	 order by  a.PROD_CATE_LEV2_NAME
	*/ 
	/*--1.6.2 品类销售结构(20180814-20190815)*/
	/*
	select 	
		PROD_CATE_LEV1_NAME,
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_times) as sale_times
		,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
	from  t2_1 c
	group by 
		PROD_CATE_LEV1_NAME
	order by 
		PROD_CATE_LEV1_NAME
	 */
  /*--1.6.3 年龄中各品类销售占比(20180814-20190815)*/
	 /*
	 ,t3 as (
	select 	
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end as  age, --"年龄"
PROD_CATE_LEV1_NAME,
sum(sale_amt) as sale_amt         --总销售额
from  t2_1 c
group by 
PROD_CATE_LEV1_NAME,
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end
order by 
	 PROD_CATE_LEV1_NAME,
	 case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end)
	 
	,t4 as 
 (select 	
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end as  age, --"年龄"
sum(sale_amt) as total_sale_amt         --总销售额
from  t2_1 c
group by 
case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end
order by 
	 case
   when floor(days_between(c.birthday,now())/365)<=20 then '20以下'
   when floor(days_between(c.birthday,now())/365)>20 and  floor(days_between(c.birthday,now())/365)<=25 then '20<d<=25'
   when floor(days_between(c.birthday,now())/365)>25 and  floor(days_between(c.birthday,now())/365)<=30 then '25<d<=30'
   when floor(days_between(c.birthday,now())/365)>30 and  floor(days_between(c.birthday,now())/365)<=35 then '30<d<=35'
   when floor(days_between(c.birthday,now())/365)>35 and  floor(days_between(c.birthday,now())/365)<=40 then '35<d<=40'
    when floor(days_between(c.birthday,now())/365)>40 and  floor(days_between(c.birthday,now())/365)<=45 then '40<d<=45'
     when floor(days_between(c.birthday,now())/365)>45 and  floor(days_between(c.birthday,now())/365)<=50 then '45<d<=50'
     when floor(days_between(c.birthday,now())/365)>50 and  floor(days_between(c.birthday,now())/365)<=55 then '50<d<=55'
     when floor(days_between(c.birthday,now())/365)>55 and  floor(days_between(c.birthday,now())/365)<=60 then '55<d<=60'
     when floor(days_between(c.birthday,now())/365)>60 and  floor(days_between(c.birthday,now())/365)<=65 then '60<d<=65'
     when floor(days_between(c.birthday,now())/365)>65 and  floor(days_between(c.birthday,now())/365)<=70 then '65<d<=70'
     when floor(days_between(c.birthday,now())/365)>70 and  floor(days_between(c.birthday,now())/365)<=75 then '70<d<=75'
	 when floor(days_between(c.birthday,now())/365)>75 and  floor(days_between(c.birthday,now())/365)<=80 then '75<d<=80'
	 when floor(days_between(c.birthday,now())/365)>80 and  floor(days_between(c.birthday,now())/365)<=85 then '80<d<=85'
	 else  '85以上'  end)
	 
	 --计算每个品类在年龄段中的销售占比
	 select 
	 t3.age,
	 t3.PROD_CATE_LEV1_NAME,
     t3.sale_amt/t4.total_sale_amt         --占比
	 from t3 
	 left join t4 
	 on t3.age=t4.age
	 order by 
	  t3.age,
	 t3.PROD_CATE_LEV1_NAME;
	 */
  /*--1.6.4 品类趋势（20150101-20190815）*/
  /*	select 	
		AT_YEAR,
		PROD_CATE_LEV2_NAME,
		count(distinct member_id)  as memb_qty                           --"会员数"
		,sum(sale_times) as sale_times
		,sum(sale_times)/count(distinct member_id) as memb_avg_ordr    --人均消费频次
		,sum(sale_amt) as sale_amt         --总销售额
		,sum(GROS_PROF_AMT) as GROS_PROF_AMT   --总毛利额
	from  t2_1 c
		group by 
		AT_YEAR,
		PROD_CATE_LEV2_NAME
		order by 
		AT_YEAR,
	 PROD_CATE_LEV2_NAME; 
	
	/*--1.7 疾病分布(20180814-20190815)*/
	/*
	,t1_2 as (
			select
				stsc_date,
				member_id,
				phmc_code,
				max(IS_YY) as IS_YY,
				MAX(PHMC_TYPE) As PHMC_TYPE,
				max(birthday) as birthday,
				max(sex) as sex,
				max(create_time) as create_time,
				sum(sale_amt) as sale_amt,          --销售额
				sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
				count (distinct GOODS_CODE) as goods_qty,  --商品类型数量
				1 as sale_times     --消费次数/订单数（同人同天同门店算一次）
			from  t1_1
			where is_member='Y' and IS_SALE_YY='Y' AND IS_YY='Y' AND SALE_PHMC_FLAG='自营' and OPEN_PHMC_FLAG='自营' 
			group by 
			stsc_date,
			member_id,
			phmc_code)    	
   

	,t1_3 as 
	(		
		select 
			member_id,
			FIR_DISEASE_NAME as DISEASE_NAME
		from "DM"."MEMBER_FOCU_DISEASE_LAB" 
		union 
		select
			member_id,
			SECOND_DISEASE_NAME as DISEASE_NAME
		from "DM"."MEMBER_FOCU_DISEASE_LAB"
		)
		
		select 
		DISEASE_NAME,
		--sex,
		--case when floor(days_between(birthday,now())/365)>30 and  floor(days_between(birthday,now())/365)<=45 then '30<d<=45'
				-- when floor(days_between(birthday,now())/365)>45 and  floor(days_between(birthday,now())/365)<=55 then '45<d<=55'
				-- when floor(days_between(birthday,now())/365)>55 and  floor(days_between(birthday,now())/365)<=85 then '55<d<=85'
				 --else  '其他'  end as  age,                                            --"年龄"
		count(distinct a.member_id),
		sum(sale_amt) as sale_amt,          --销售额
		sum(GROS_PROF_AMT) as GROS_PROF_AMT,    --毛利
        sum(sale_amt)/count(distinct a.member_id), --人均销售
        sum(GROS_PROF_AMT)/count(distinct a.member_id) --人均毛利
		from t1_3 a
		inner join t1_2 b
		on a.member_id=b.member_id
		--where floor(days_between(birthday,now())/365)>=20 and floor(days_between(birthday,now())/365)<=85
		--and sex in ('男','女')
		group by 
		DISEASE_NAME
		--sex,
		--case when floor(days_between(birthday,now())/365)>30 and  floor(days_between(birthday,now())/365)<=45 then '30<d<=45'
		      --when floor(days_between(birthday,now())/365)>45 and  floor(days_between(birthday,now())/365)<=55 then '45<d<=55'
			-- when floor(days_between(birthday,now())/365)>55 and  floor(days_between(birthday,now())/365)<=85 then '55<d<=85'
			-- else  '其他'  end
			; 
    */	
--门店概览
	select 
      PHMC_TYPE,
       case when open_month>'201808' then '开业1年以内'
            when open_month<='201808' AND open_month>'201608' then '开业1-3年'
            when open_month<='201608' then '开业3年以上' END AS OPEN_MONTH,
       COUNT(*)
    from "EXT_TMP"."YUEYANG_STORE" 
    GROUP BY  PHMC_TYPE,
       case when open_month>'201808' then '开业1年以内'
            when open_month<='201808' AND open_month>'201608' then '开业1-3年'
            when open_month<='201608' then '开业3年以上' END
            