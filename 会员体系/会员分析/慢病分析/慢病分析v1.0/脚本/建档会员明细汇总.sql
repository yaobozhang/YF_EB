

--造数据
with t0_1 as (
	select add_days(to_date('2017-10-01','yyyy-mm-dd'),Row_Number() OVER(ORDER BY code desc)-1) dates
	from "DW"."BI_TEMP_COUPON_ALL" limit 638
	)
,
--STEP0:建档会员信息
 t0_2 as (
select
card_code
,to_date(min(create_time)) as mb_date
from "DS_ZT"."ZT_CHRONIC_BASELINE"
where card_code is not null
group by
card_code
)
,

t0 as (
select
card_code
,mb_date
,dates
from t0_2 t1
inner join t0_1 t2 on 1=1
)

select
to_char(dates,'yyyymm') mon 
count(distinct )

--select dates from t0 limit 100
,

--STEP1:建档会员消费情况
-- 建档会员消费明细
t1_1 as (
   select 
		 d.memb_code as card_code 	
		,to_char(stsc_date,'yyyymmdd') stsc_date	--消费日期
		,count(1) over(partition by memb_code, stsc_date ) xf_num --消费次数
		,SALE_AMT		--每天消费金额
		,SALE_GROS			--每天贡献毛利额
   from EXT_TMP.HY_SALE d 
   where d.memb_code is not null
 )
 ,
 --建档会员消费汇总——到日
 t1 as (
  select
	 card_code
	 ,stsc_date 
	 ,'1' as dd_type		--类型 1代表消费
	 ,max(xf_num)	dd_num		--到店次数
	 ,max(xf_num)	xf_num	--消费次数
	,sum(SALE_AMT) SALE_AMT	--消费金额
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
	select  card_code		--建档会员
		 ,stsc_date		--日期
		,count(1) over(partition by card_code, stsc_date ) dd_num --到店次数
		,xf_num				--消费次数
		,SALE_AMT			--消费金额
		,SALE_GROS			--消费毛利
	from
	(
		select	
			t.card_code	--建档会员	
			 ,to_char(t4.RECORD_date,'yyyymmdd') stsc_date		--监测日期
			 ,0 AS SALE_AMT			
			 ,0 AS SALE_GROS
			,0 as xf_num
		 from 
		 "DS_ZT"."ZT_CHRONIC_BASELINE" t
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORDER" t3 on t.customer_id=t3.customer_id 
		 inner join "DS_ZT"."ZT_MEDSERVICE_RECORD" t4 on t4.recorder_id=t3.id 
		 where t4.IS_DELETE='1'
		 and t4.record_from = 'MB_ST'
		 and t.card_code is not null
	 )
 --limit 1000
 )
 
,
--建档会员监测汇总
t2 as (
  select
	 card_code
	 ,stsc_date 
	 ,'2' as dd_type		--类型 1代表消费
	 ,max(dd_num)	dd_num		--到店次数
	 ,max(xf_num)	xf_num	--消费次数
	,sum(SALE_AMT) SALE_AMT	--消费金额
	,sum(SALE_GROS) SALE_GROS	--消费毛利
 from t2_1
 group by 
	card_code 	--一个人一天消费情况
	,stsc_date	
) 


,

--STEP3:建档会员到店情况
t3_1 as (
select 	 card_code
	 ,stsc_date 
	 ,dd_type		--类型 1代表消费
	 ,	dd_num		--到店次数
	 ,	xf_num	--消费次数
	, SALE_AMT	--消费金额
	, SALE_GROS	--消费毛利
from t1 
union all
select 	card_code
	 ,stsc_date 
	 ,dd_type		--类型 1代表消费
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
	 ,case when flag>1 then '3' else dd_type end as dd_type		--类型 1代表消费
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
--select stsc_date from t3  limit 100
,

--汇总——到日
t4 as (
		select
		t1.card_code	--会员卡号
		,t1.mb_date	--建档时间
		,t1.dates	--统计日期
		,t2.dd_num		--到店次数
		,t2.xf_num	--消费次数
		,t2.SALE_AMT	--消费金额
		--,SALE_GROS	--消费毛利
		from t0 t1
		left join t3 t2 on t1.card_code = t2.card_code and t1.dates = t2.stsc_date
		where t1.dates>=t1.mb_date		--统计日期晚于建档时间
)
select count(distinct card_code) from t4
,

--统计建档会员每个月情况
t5 as (
select
 to_char(dates,'yyyymm') as mon			--日期
,rank() OVER (ORDER BY to_char(dates,'yyyymm')) Xth_mon
,count(distinct case when to_char(mb_date,'yyyymm') = to_char(dates,'yyyymm') then CARD_CODE end) jd_memb --建档人数
,count(distinct case when dd_num >0 then CARD_CODE end) as memb_dd_num		--到店人数
,count(distinct case when xf_num >0 then CARD_CODE end) as memb_xf_num		--消费人数
,sum(dd_num) as dd_times		--到店次数
,sum(xf_num) as xf_times	--消费次数
,sum(SALE_AMT) as SALE_AMT	--消费金额
--,sum(SALE_GROS)	 as SALE_GROS	--消费毛利
from t4
group by 
 to_char(dates,'yyyymm')
)
,

t6 as (
select
mon
,Xth_mon
--,sum(jd_memb) OVER (partition by Xth_mon) sum_jd_people_num	
,jd_memb
,memb_dd_num
,memb_xf_num
,ifnull(dd_times,0) dd_times
,ifnull(xf_times,0) xf_times
,ifnull(SALE_AMT,0) SALE_AMT
from t5
)
,
t7 as (
	select
	t1.mon
	,t1.Xth_mon
	,t2.sum_jd_memb
	,t1.jd_memb
	,t1.memb_dd_num
	,t1.memb_xf_num
	,t1.dd_times
	,t1.xf_times
	,t1.SALE_AMT
	from t6 t1
	inner join (
		select 
		t.mon
		,t.Xth_mon
		,sum(t1.jd_memb) sum_jd_memb
		--,sum(t1.jd_memb) jd_memb
		from t6 t
		left join t6 t1 on t.Xth_mon>=t1.Xth_mon
		group by	
		t.mon
		,t.Xth_mon
		) t2 on t1.mon = t2.mon and t1.Xth_mon = t2.Xth_mon
		
)

		
		
select
	t1.mon  		--月份
	,t1.Xth_mon		--第X月
	,t1.sum_jd_memb	--累计建档人数
	,t1.jd_memb		--建档人数
	,t1.memb_dd_num	--到店人数
	,t1.memb_xf_num	--消费人数
	,t1.dd_times	--到店次数
	,t1.xf_times	--消费次数
	,t1.SALE_AMT	--消费金额
	,t1.dd_times/t1.memb_dd_num		avg_dd_times--人均到店次数
	,t1.xf_times/t1.memb_xf_num		avg_xf_times--人均消费次数
	,t1.SALE_AMT/t1.memb_xf_num		avg_SALE_AMT--人均消费金额
from t7 t1
order by 
mon