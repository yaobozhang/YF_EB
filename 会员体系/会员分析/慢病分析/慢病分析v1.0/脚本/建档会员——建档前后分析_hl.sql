
drop table EXT_TMP.HY_SALE;				--删除消费明细表
CREATE TABLE EXT_TMP.HY_SALE AS (		--创建建档会员的消费明细表，作为建档会员建档前后X个月的消费情况的基础
	select
			p.MEMB_CODE					--会员卡号	
			,p.STSC_DATE				--消费日期
			,p.phmc_code				--门店编号
			,sum(SALE_AMT) SALE_AMT		--消费金额
			,SUM(GROS_PROF_AMT) AS SALE_GROS		--销售毛利
			from DW.FACT_SALE_ORDR_DETL p
		   left join dw.DIM_GOODS_H g on g.goods_sid=p.goods_sid
		   where p.stsc_date >= '2016-10-1'		--时间截取2016年10月
		   and  p.stsc_date < '2019-7-1' 		--到2019年6月
		   and "ORDR_CATE_CODE"<>'3'			--剔除订单类型为3，4的订单
		   and "ORDR_CATE_CODE"<>'4'		
		   and "PROD_CATE_LEV1_CODE"<>'Y85'		--剔除赠品
		   and "PROD_CATE_LEV1_CODE"<>'Y86'		--剔除服务型商品
		   and g."GOODS_CODE" <> '8000875'		
		   and g."GOODS_CODE" <> '8000874' 
		   and p.member_id is not null			--选取会员
		   and exists(							--只选取建档会员的消费明细
				select 1 from
				(
					select
					card_code
					,min(create_time) mb_date
					from "DS_ZT"."ZT_CHRONIC_BASELINE" 
					group by
					card_code
				) t2
				where p.MEMB_CODE=t2.card_code
		   )
		   group by p.MEMB_CODE
			,p.STSC_DATE
			,p.phmc_code			
)


--构建月数
with t0_1 as (							--构建建档前后12个月，用于匹配建档会员建档前后X个月的消费到店等情况	
select
Xth_mon
from (
select Row_Number() OVER(ORDER BY code desc)-13 Xth_mon
 from "DW"."BI_TEMP_COUPON_ALL" limit 25
 )
 where Xth_mon<> 0 
)
,
--STEP0.2:建档会员信息
t0_2 as (								--获取建档会员的卡号和建档时间，用于计算建档前后X个月
select
card_code
,to_date(min(create_time)) as mb_date
from "DS_ZT"."ZT_CHRONIC_BASELINE"
where card_code is not null
group by
card_code
)
,
--构建建档会员建档前后12个月的模板
t0_3 as (
select
card_code
,Xth_mon
from t0_2 
inner join t0_1 on 1=1
)
,

--STEP1:建档会员消费情况
-- 建档会员消费明细
t1_1 as (
   select 
		 d.memb_code as card_code 	--建档会员卡号
		,to_char(stsc_date,'yyyymmdd') stsc_date	--消费日期
		,count(1) over(partition by memb_code, stsc_date ) xf_num --每个建档会员的消费次数
		,SALE_AMT		--每个建档会员每天的消费金额
		,SALE_GROS			--每个建档会员每天的贡献毛利额
   from EXT_TMP.HY_SALE d 
   where d.memb_code is not null
  )
 ,
 --建档会员消费汇总——到日
 t1 as (
  select
	 card_code					--建档会员卡号
	 ,stsc_date 				--消费日期
	 ,'1' as dd_type			--类型 1代表到店行为为消费
	 ,max(xf_num)	dd_num		--到店次数
	 ,max(xf_num)	xf_num		--消费次数
	,sum(SALE_AMT) SALE_AMT		--消费金额
	,sum(SALE_GROS) SALE_GROS	--消费毛利
 from t1_1
 group by 
	card_code 	--一个人一天消费情况
	,stsc_date	
 )

 
 ,
 --STEP2:建档会员建档后到店情况
--建档会员建档后监测日期
t2_1 as (
	select  card_code		--建档会员卡号
		 ,stsc_date			--监测日期
		,count(1) over(partition by card_code, stsc_date ) dd_num --每个建档会员每天到店次数
		,xf_num				--消费次数
		,SALE_AMT			--消费金额
		,SALE_GROS			--消费毛利
	from
	(
		select	
			t.card_code			--建档会员	
			 ,to_char(t4.RECORD_date,'yyyymmdd') stsc_date		--监测日期
			 ,0 AS SALE_AMT		--构造消费金额，便于到店次数的计算	
			 ,0 AS SALE_GROS	--构造销售毛利，便于到店次数的计算
			,0 as xf_num		--构造消费次数，便于到店次数的计算
		 from 
		 "DS_ZT"."ZT_CHRONIC_BASELINE" t
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORDER" t3 on t.customer_id=t3.customer_id 
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORD" t4 on t4.recorder_id=t3.id 
		 where t4.IS_DELETE='1'	
		 and t4.record_from = 'MB_ST'		--到门店
		 and t.card_code is not null		--建档会员卡号不为空
	 )
 --limit 1000
 )
 
,
--建档会员监测汇总
t2 as (
  select
	 card_code			--建档会员卡号
	 ,stsc_date 		--监测日期
	 ,'2' as dd_type	--类型 2代表到店行为为测血糖血压
	 ,max(dd_num)	dd_num		--到店次数
	 ,max(xf_num)	xf_num		--消费次数
	,sum(SALE_AMT) SALE_AMT		--消费金额
	,sum(SALE_GROS) SALE_GROS	--消费毛利
 from t2_1
 group by 
	card_code 	--一个人一天监测情况
	,stsc_date	
) 
,


--STEP3:建档会员到店情况		合并消费情况和监测情况，用于汇总到店情况
t3_1 as (
select 	 card_code
	 ,stsc_date 
	 ,dd_type		--类型 1
	 ,	dd_num		--到店次数
	 ,	xf_num	--消费次数
	, SALE_AMT	--消费金额
	, SALE_GROS	--消费毛利
from t1 
union all
select 	card_code
	 ,stsc_date 
	 ,dd_type		--类型 2
	 ,	dd_num		--到店次数
	 ,	xf_num	--消费次数
	, SALE_AMT	--消费金额
	, SALE_GROS	--消费毛利
from t2
)
,
t3_2 as
(
	select 	card_code
	 ,stsc_date 
	 ,count(1) over(partition by card_code, stsc_date ) flag1	--判断flag
	,dd_type
	 ,dd_num		--到店次数
	 ,xf_num	--消费次数
	,SALE_AMT	--消费金额
	,SALE_GROS	--消费毛利
	from t3_1
)
,
--聚合得到消费及监测合并后结果
t3 as (
	select 	card_code
	 ,stsc_date 
	 ,case when flag>1 then '3' else dd_type end as dd_type		--类型 3代表到店行为既有消费又有监测
	 ,dd_num		--到店次数
	 ,xf_num	--消费次数
	,SALE_AMT	--消费金额
	,SALE_GROS	--消费毛利
	from
	(
		select 	card_code
		 ,stsc_date
		 ,max(flag1) as flag
		 ,max(dd_type) as dd_type
		 --,case when count(1)>1 then '3' else dd_type end as dd_type		--类型 1代表消费
		 ,max(dd_num) as dd_num		--到店次数
		 ,max(xf_num) as xf_num	--消费次数
		, sum(SALE_AMT) as SALE_AMT	--消费金额
		, sum(SALE_GROS) as SALE_GROS	--消费毛利
		from t3_2
		group by card_code,stsc_date
	)

)
,
--关联得到建档时间			--用于计算建档前后X个月的到店情况
t4 as(
	select
		card_code
		 ,Xth_mon 
		 ,sum(dd_num) as dd_num	--到店次数
		 ,sum(xf_num) as xf_num	--消费次数
		, sum(SALE_AMT) as SALE_AMT 	--消费金额
		, sum(SALE_GROS) as SALE_GROS	--消费毛利
	from
	(
		select 	 t3.card_code
			 ,t3.stsc_date 
			 ,t3.dd_type		
			 ,	t3.dd_num		--到店次数
			 ,	t3.xf_num	--消费次数
			, t3.SALE_AMT	--消费金额
			, t3.SALE_GROS	--消费毛利
			,t4.mb_date	--建档时间
			,case when t3.stsc_date>=t4.mb_date then months_between(t4.mb_date,t3.stsc_date)+1		--建档后
			else months_between(t4.mb_date,t3.stsc_date) end Xth_mon						--建档前
		from t3
		left join t0_2 t4
		on t3.card_code=t4.card_code
	)
	group by card_code
		 ,Xth_mon 
	
)
,
--关联得到每个人前后12月表现明细
t5 as(
	select
	t1.card_code	--卡号
	,t1.Xth_mon	--建档前后
	,ifnull(t4.dd_num,0)  as dd_num	--到店次数
	,ifnull(t4.xf_num,0)  as xf_num	--消费次数
	,ifnull(t4.SALE_AMT,0)  as SALE_AMT 	--消费金额
	,ifnull(t4.SALE_GROS,0)  as SALE_GROS	--消费毛利
	from t0_3 t1
	left join t4
	on t1.card_code=t4.card_code
	and t1.Xth_mon=t4.Xth_mon
	
)
,
--关联得到每个月建档门店
t6_1 as (
		select
	t1.card_code	--卡号
	,t1.Xth_mon	--建档前后
	,t1.dd_num	--到店次数
	,t1.xf_num	--消费次数
	,t1.SALE_AMT 	--消费金额
	,t1.SALE_GROS	--消费毛利
	,t2.store_code
	from t5 t1
	inner join "DS_ZT"."ZT_CHRONIC_BASELINE" t2 on t1.card_code = t2.card_code
	where t2.store_code is not null
)
,
--门店过滤，得到分析门店数据及店型
t6 as(
	select
	t1.card_code	--卡号
	,t1.Xth_mon	--建档前后
	,t1.dd_num	--到店次数
	,t1.xf_num	--消费次数
	,t1.SALE_AMT 	--消费金额
	,t1.SALE_GROS	--消费毛利
	,t1.store_code		--建档门店
	,t2."phmc_type"		--门店类型
	from t6_1 t1
	inner join "EXT_TMP"."phmc_mb_all" t2
	on t1.store_code=t2."phmc_code"
	where t2."phmc_code" is not null
)

select 
	--"phmc_type"
	--,store_code
	Xth_mon
	,sum(case when dd_num >0 then 1 else 0 end) as memb_dd_num
	,sum(case when xf_num >0 then 1 else 0 end) as memb_xf_num
	,sum(dd_num)_dd_num		--到店频次
	,sum(xf_num)xf_num		--消费频次
	,sum(SALE_AMT)SALE_AMT	--销售金额
	,sum(SALE_GROS) SALE_GROS	
from t6
group by --"phmc_type",store_code,
Xth_mon
order by --"phmc_type",store_code,
Xth_mon
limit 4000
,
--step7:大聚合，得到每种维度下数据
--先看总的建档前后数据
t7_1 as (
	select 
	"phmc_type"
	,Xth_mon
		,sum(dd_num)/sum(case when dd_num >0 then 1 else 0 end) as memb_avg_dd_num		--人均到店频次
		,sum(xf_num)/sum(case when xf_num >0 then 1 else 0 end) as memb_avg_xf_num		--人均消费频次
		,sum(SALE_AMT)/sum(case when SALE_AMT >0 then 1 else 0 end) as memb_avg_SALE_AMT	--人均销售金额
		,sum(SALE_GROS)/sum(case when SALE_AMT >0 then 1 else 0 end) as memb_avg_SALE_GROS	--人均销售毛利
	from t6
	group by "phmc_type",Xth_mon
	
)

select * from t7_1


