--该代码为会员分类分级数据分析部分
--代码贡献者：姚泊彰
--时间：20190609

--STEP1：得到订单条件，并进行门店过滤
with t1_1 as
(
	select member_id
		,PHMC_CODE
		,stsc_date
		--,max()
		,SUM(SALE_AMT) AS SALE_AMOUNT		--每单消费金额
		,SUM(GROS_PROF_AMT) AS SALE_GROS			--每单贡献毛利额
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--营运剔除服务性商品
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--营运去除品类
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--营运去除单品
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--分析会员复购周期
	and not exists		--门店过滤：1、开店大于分析日期；2、上海医保；3、关停
	(
		select 1 from dw.DIM_PHMC g1
		where g1.PHMC_CODE = s.PHMC_CODE 
		and 
		(	--上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
			g1.STAR_BUSI_TIME >= '20190601' 
			or (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
			or CLOSE_DATE is not null
			or PROP_ATTR in ('Z02','Z07')		--收购
			or company_code='4000'			--加盟
		 )
	)
	group by member_id,PHMC_CODE,stsc_date	--每个会员每天在每个门店算一单
)
,

--进行会员过滤
t1 as
(
	select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT		--每单消费金额
		,s.SALE_GROS			--每单贡献毛利额
	from t1_1 s
	where not exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and c.come_from in ('SG','JM')
	)
	AND exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
	)
)
,
--step2:对过滤后的结果进行处理，得到每笔单在每个人消费中的排序
t2 as
(
	select member_id
		,PHMC_CODE
		,stsc_date
		,row_number() over(partition by member_id order by stsc_date asc,PHMC_CODE asc) as memb_order_rank
	from t1
)

select 365/(count(1)/count(distinct member_id)) as avg_days from t2

,
--得到分析复购率基本数据形式：每个人每笔单及后一笔单的情况
t3 as
(
	select t1.member_id
		,t1.PHMC_CODE
		,t1.stsc_date
		,t1.memb_order_rank
		,t2.stsc_date as stsc_date_after
		,case when t2.stsc_date is not null then days_between(t1.stsc_date,t2.stsc_date) else null end as buy_time_dis
		,count(1) OVER() as total_order
	from 
	(select member_id
		,PHMC_CODE
		,stsc_date
		,memb_order_rank
		,memb_order_rank+1 as memb_order_rank_2
		from t2
	) t1
	left join 
	t2 t2
	on t1.member_id=t2.member_id
	and t1.memb_order_rank_2=t2.memb_order_rank
)
--select avg(buy_time_dis) from t3
,
--开始统计分析
t4 as
(
	select Row_Number() OVER(ORDER BY code desc)-1 as day_diff from "DW"."BI_TEMP_COUPON_ALL" limit 366
)
,
--得到每个间隔天数的复购率
t5 as
(
	select t4.day_diff
		,sum(case when t4.day_diff>=t3.buy_time_dis then 1 else 0 end)/max(total_order) as back_rate	--复购率
	from t4
	left join t3
	on 1=1
	group by t4.day_diff
)
select * from t5


-----------------------------------以下部分是数据分级成长值数据，得到等级间周期---------------------------
with t1_1 as
(
	select member_id
		,PHMC_CODE
		,stsc_date
		--,max()
		,SUM(SALE_AMT) AS SALE_AMOUNT		--每单消费金额
		,SUM(GROS_PROF_AMT) AS SALE_GROS			--每单贡献毛利额
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2019-06-01',-1)
	and s.stsc_date<'2019-06-01'
	and "ORDR_CATE_CODE"<>'3'		--营运剔除服务性商品
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--营运去除品类
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--营运去除单品
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--分析会员复购周期
	and not exists		--门店过滤：1、开店大于分析日期；2、上海医保；3、关停
	(
		select 1 from dw.DIM_PHMC g1
		where g1.PHMC_CODE = s.PHMC_CODE 
		and 
		(	--上海公司医保店或者开店时间大于20190501或者有关店时间的剔除
			g1.STAR_BUSI_TIME >= '20190601' 
			or (g1.PHMC_S_NAME like '%医保%' and g1.ADMS_ORG_CODE = '1001' )
			or CLOSE_DATE is not null
			or PROP_ATTR in ('Z02','Z07')		--收购
			or company_code='4000'			--加盟
		 )
	)
	group by member_id,PHMC_CODE,stsc_date	--每个会员每天在每个门店算一单
)
,


--进行会员过滤
t1 as
(
	select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT		--每单消费金额
		,s.SALE_GROS			--每单贡献毛利额
	from t1_1 s
	where not exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and c.come_from in ('SG','JM')
	)
	AND exists
	(
		select 1 from ds_crm.tp_cu_customerbase c
		where s.member_id=c.customer_id
		and create_time<'2018-06-01'
	)
)
,
t2 as
(
	select 
		sum(SALE_AMOUNT)SALE_AMOUNT,
		CCZ,
		count(1) mt
	from 
	(
		select 
			 member_id
			 ,FLOOR(sum(SALE_AMOUNT))CCZ
			 ,sum(SALE_AMOUNT) SALE_AMOUNT
		from
			t1 s 
		group by 
			member_id
	)
	where SALE_AMOUNT>=0
	group by CCZ
)


---------------------------

,
t5 as (
	select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT		--每单消费金额
		,row_number() OVER (PARTITION BY member_id ORDER BY stsc_date asc) rk
	from (select s.member_id
		,s.PHMC_CODE
		,s.stsc_date
		,s.SALE_AMOUNT from t1 s where SALE_AMOUNT>0
		)s
)
,

t6 as 
(
	select 
	member_id,
	min(stsc_date)stsc_date,
	case when SALE_AMOUNT<100 then 1
	when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
	when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
	when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
	when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
	when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
	when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
	when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
	when SALE_AMOUNT>=4600 and SALE_AMOUNT<7000 then 10
	when SALE_AMOUNT>=7000 and SALE_AMOUNT<10000 then 11
	when SALE_AMOUNT>=10000 then 12 end as LV 
	from
	(
		select 
		t.member_id
		,t.stsc_date
		,sum(t1.SALE_AMOUNT) SALE_AMOUNT
		from t5 t
		left join t5 t1 on t1.member_id=t.member_id and t.rk>=t1.rk
		group by t.member_id
		,t.stsc_date
	)
	group by member_id,
	case when SALE_AMOUNT<100 then 1
	when SALE_AMOUNT>=100 and SALE_AMOUNT<200 then 2
	when SALE_AMOUNT>=200 and SALE_AMOUNT<400 then 3
	when SALE_AMOUNT>=400 and SALE_AMOUNT<600 then 4
	when SALE_AMOUNT>=600 and SALE_AMOUNT<900 then 5
	when SALE_AMOUNT>=900 and SALE_AMOUNT<1200 then 6
	when SALE_AMOUNT>=1200 and SALE_AMOUNT<1600 then 7
	when SALE_AMOUNT>=1600 and SALE_AMOUNT<2500 then 8
	when SALE_AMOUNT>=2500 and SALE_AMOUNT<4600 then 9
	when SALE_AMOUNT>=4600 and SALE_AMOUNT<7000 then 10
	when SALE_AMOUNT>=7000 and SALE_AMOUNT<10000 then 11
	when SALE_AMOUNT>=10000 then 12 end
)	
,
--关联得到差值
t8 as
(
	select T1.member_id
		,t1.LV
		,t2.LV as lv_2
		,case when t2.LV IS NULL THEN 0 ELSE DAYS_BETWEEN(t2.stsc_date,t1.stsc_date) end as day_diff
	from 
	(
		select member_id,
			stsc_date,
			LV,
			LV-1 AS LV_BEFORE
		from t6
		WHERE lv>1
	) t1 
	left join t6 t2
	on t1.member_id=t2.member_id
	and t1.LV_BEFORE=t2.LV
)
,t9 as
(
	select LV,sum(day_diff)/count(case when LV_2 is not null then member_id end)  as day_diff_avg
	from t8
	GROUP BY LV
)
select * from t9