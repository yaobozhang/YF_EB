--开发者：姚泊彰
--问题：每月固定分析主流券用券未用券情况
--注意点：1、本脚本分为三段，实现三种不同看数方式 2、脚本可以直接运行，如果要当月数据请修改参数
--是否需要修改参数：是  需要修改处有备注
--开发时间：20190103
--修改记录
--
-------------------------------------------------PART1 分析每月总体用券情况-------------------------------------------------
--STEP1：得到当月发券数据---
with t1 as
(
select a.member_id,coupon_flag,coupon_logical_flag,a.coupontype,a.code,to_date(used_time) used_time,daydiff,week_day
	from
		(
		select member_id,code,coupon_flag,coupon_logical_flag,update_date_send,coupontype
		from "DM"."BI_MEMB_COUPON_COMBINE_HIST"
		where to_date(update_date_prod)>='20181201' and to_date(update_date_prod)<='20181231'					--参数修改处
		and coupon_flag in ('RE_PURCH','WAKE_UP','NEW')
		and promotionsource='B'
		) a
	inner join
		(select customer_id member_id,template_id code,begin_time,expire_time,used_time,create_time,weekday(used_time)+1 as week_day
		,order_code,days_between(begin_time,used_time) daydiff
		 from "DS_CRM"."ZT_COUPON" 
		 where used_time is not null 
		 and to_date(used_time)>='20181201' and to_date(used_time)<='20181231'						--参数修改处
		) b
	on a.member_id=b.member_id and a.code=b.code 
	where a.update_date_send<=b.create_time and add_days(a.update_date_send,3)>=b.create_time

),
--step2:查看发券用券时间差分布
t2 as  --分析有效期内用券分布(新客、老客、流失)
(
	select coupon_flag,daydiff,count(1)
	from t1
	group by coupon_flag,daydiff
	order by coupon_flag,daydiff asc

),
--step3:查看周内用券时间分布
t3 as --分析周内发券分布
(
	select coupon_flag,week_day,count(1)
	from t1
	group by coupon_flag,week_day
	order by coupon_flag,week_day asc
),
--step4:查看每种券类型发券用券时间差详细情况
t4 as
(
	select coupon_flag,coupontype,daydiff,count(1)
	from t1
	group by coupon_flag,coupontype,daydiff
	order by coupon_flag,coupontype,daydiff asc

)
--select * from t2
--select * from t3
select * from t4	--分别查看各项数据


-------------------------------------------------------------------PAR2 查看各种用券未用券统计结果-------------------------------------------
--取本月老客用券情况详细订单数据
--先取所有数据
with x1 as --找到主流券用户的用券信息	
(
		select a.member_id,a.code,a.coupon_flag,a.coupon_logical_flag,a.update_date_send,b.create_time,b.create_date,b.begin_time,b.expire_time,b.used_time,b.order_code
		from
			(select member_id,code,coupon_flag,coupon_logical_flag,update_date_send
			from "DM"."BI_MEMB_COUPON_COMBINE_HIST" 
			where --coupon_logical_flag in ('chron_7','normal_7') and
			coupon_flag in ('RE_PURCH','NEW','WAKE_UP') and
			promotionsource='B' and
			update_date_prod>='20181201' and update_date_prod<='20181231'		--参数修改处
			) a
		inner join --关联电商用户持券表
			(select customer_id member_id,template_id code,begin_time,create_time,to_date(create_time) create_date,expire_time,used_time,order_code
			 from "DS_CRM"."ZT_COUPON" where begin_time>='20181201' and begin_time<='20181231'		--参数修改处
			) b
			on a.member_id=b.member_id and a.code=b.code 
			where a.update_date_send=b.create_date
		)	
,
x2 as --找到用户实际上有券并在有效期回头的信息
(
	select t1.member_id,t1.code,t1.coupon_flag,t1.coupon_logical_flag
	,t1.begin_time,t1.expire_time,t1.used_time,t1.order_code
	,case when t2.sale_time>=t1.begin_time and t2.sale_time<=t1.expire_time then t2.sale_time else
	null end as sale_time
	,t2.sale_price_money
	,row_number() over(partition by t1.member_id order by t2.sale_time asc) as rn  --找到回头订单取第一单
	,sale_bill_code
	from x1 t1
	left join
		(--为减少数据量，取9月1日后的订单数据
		select member_id,sale_time,sale_price_money,sale_bill_code
		from "DS_POS"."SALES_ORDER"
		where sale_time>='20181201' and sale_time<='20181231'		--参数修改处
		) t2
	on t1.member_id=t2.member_id
	and t1.create_time<=t2.sale_time and t1.expire_time>=t2.sale_time
	),
x3 as --匹配得到订单包含商品，券信息，券商品信息等信息
(
	select t1.member_id --用户id
,t1.code --券码
,t1.coupon_flag --生命周期
,t1.coupon_logical_flag --逻辑标识
,t1.begin_time --券生效时间
,t1.expire_time --券失效时间
,t1.used_time --券使用时间，如果为空代表未用券
,t1.order_code --订单号
,t1.sale_time  --订单产生时间
,t1.sale_price_money --订单金额
,t1.sale_bill_code --订单详情号
,t2.coupontype coupon_type--券类型
,t2.usecondition use_condition--使用门槛
,t2.discountmoney coupon_money--折扣力度
,t3.good_code--购买商品
,t3.quantity --购买数量
,t3.sale_money --购买商品金额
,t2.SINGLEGOODSCODE good_id--券模板商品
,t2.quantity quantity_id --推荐数量
from
	 x2 t1
left join--取购买商品code
 "DS_POS"."SALES_ORDERDETAILS" t3
 on t1.sale_bill_code=t3.sale_bill_code
left join	--取用券条件，券类型，折扣力度
	"DW"."BI_TEMP_COUPON_ALL" t2
	on t1.code=t2.code
where t1.rn=1 --回头多单取第一单
),
x4 as
(
	select x3.*,t1.group_cd,t2.group_cd group_id from x3 
	left join "DW"."DIM_GOODS_SALE_CATEGORY" t1 
	on x3.good_code= t1.goods_code
	left join (select distinct code,group_cd from "DW"."BI_TEMP_DISEASE") t2
	on x3.code=t2.code
),
x5 as
(
	select member_id,group_cd,sum(sale_money) as cate_money from x4
	group by member_id,group_cd
), 
x6 as
(
	select x4.*,x5.cate_money from x4 left join x5 on x4.member_id=x5.member_id where x4.coupon_type in ('GOODS') 
),
---------开始取分析数据-----------
--全场券数据-------
y1 as
(
	select coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion,count(1) as num
	from
	(select 
	case when used_time is not null then '用券' else '未用券' end as ifuse
	,coupon_flag
	, coupon_logical_flag
	,coupon_type
	,case when used_time is null and sale_price_money>=use_condition then '超门槛'
	when used_time is null and sale_price_money<use_condition  then  '没达到'
	when used_time is null and sale_price_money is null then '没回头'
	when used_time is not null then '用券超门槛'
	end as ifcdtion
	from
		(select 
		distinct member_id,coupon_flag,coupon_logical_flag,coupon_type,used_time,sale_bill_code,sale_price_money,use_condition
		from x3
		where coupon_type in ('ALL')
		)
	)
	group by coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion
	order by coupon_flag,coupon_logical_flag,coupon_type,ifuse desc,ifcdtion 
),

----单品券数据-----------
y2 as
(	
	select coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion,count(1) as num
	from
	(select 
	case when used_time is not null then '用券' else '没用券' end as ifuse
	,coupon_flag
	, coupon_logical_flag
	,coupon_type
	,case when used_time is null and good_code=good_id and quantity>=quantity_id then '买了商品且达到数量但是没用券'
	when used_time is null and good_code=good_id and quantity<quantity_id then '买了商品没达到数量但是没用券'
	when used_time is not null then '用券买商品'
	when used_time is null and sale_price_money is null then '没回头'
	when used_time is null and sale_price_money is not null and good_code!=good_id then '回头没有购买指定商品'
	end as ifcdtion
	from
		(select * from x3 where coupon_type in ('ITEM')
		) 
	)
	group by coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion
	order by coupon_flag,coupon_logical_flag,coupon_type,ifuse desc,ifcdtion 
),

---品类券数据------------
y3 as
(	
	select coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion,count(1) as num
	from
	(select 
	case when used_time is not null then '用券' else '未用券' end as ifuse
	,coupon_flag
	, coupon_logical_flag
	,coupon_type
	,case when used_time is null and group_cd=group_id and cate_money>=use_condition then '买指定品类超门槛'
	when used_time is null and group_cd=group_id and cate_money<use_condition  then  '买指定品类没达到'
	when used_time is not null then '用券超门槛'
	else '没买指定品类或者没回头'
	end as ifcdtion
	from x6
	)
	group by coupon_flag,coupon_logical_flag,coupon_type,ifuse,ifcdtion
	order by coupon_flag,coupon_logical_flag,coupon_type,ifuse desc,ifcdtion 

),
y4 as 
(
  select * from y1
  union all
  select * from y2
  union all
  select * from y3
)
select * from y4 order by coupon_flag,coupon_logical_flag,coupon_type,ifuse desc,ifcdtion 


-------------------------------------------------------------------PAR3 查看单品中达到门槛未用券详细数据-------------------------------------------
-----直接写逻辑
select t1.member_id,t1.good_name,t1.good_code,t1.quantity as send_quantity,t1.discountmoney
,t2.quantity as buy_quantity,t2.PRMTN_BILL_NAME,t2.note,t2.ACCOUNT_PRICE,t2.retail_price,t2.PERFORM_PRICE
,t2.sale_time,t1.create_time,t1.begin_time,t1.expire_time
from
	(select t1.member_id,t1.BEGIN_TIME,t1.expire_time,t1.create_time,t2.good_code,t2.good_name,t2.quantity,t2.discountmoney
	from
		(select t1.member_id,t2.code,t2.BEGIN_TIME,t2.expire_time,t2.create_time
		 from
			(select member_id,code,update_date_send
			from
			"DM"."BI_MEMB_COUPON_COMBINE_HIST"
			where update_Date_send>='20181201' and coupontype='ITEM') t1			--参数修改处
		 inner join
			(select customer_id member_id,template_id code,create_time,BEGIN_TIME,expire_time
			,USED_TIME
			from
			"DS_CRM"."ZT_COUPON"
			where create_time>='20181201') t2				--参数修改处
		 on t1.member_id=t2.member_id
		 and t1.code=t2.code
		 where t1.update_Date_send<t2.create_time and t1.update_Date_send > add_days(t2.create_time,-3)
		 and t2.used_time is null
		) t1
		left join
		(select code,SINGLEGOODSCODE good_code,SINGLEGOODSname good_name,discountmoney,quantity 
		from "DW"."BI_TEMP_COUPON_ALL"
		) t2
		on t1.code=t2.code
)t1
inner join
(select t1.member_id,t1.good_code,t1.quantity,t1.sale_time,t2.PRMTN_BILL_NAME,t2.note,t1.ACCOUNT_PRICE,t1.retail_price,t1.PERFORM_PRICE
from
	(select t1.member_id, good_code,quantity,sale_time,PROMOTIONBILL_PLAN_CODE,t2.ACCOUNT_PRICE,t2.retail_price,t2.PERFORM_PRICE
	from
		(select member_id,sale_bill_code 
		from "DS_POS"."SALES_ORDER" where SALE_TIME>='20181201'		--参数修改处
		)t1
	inner join
		(select sale_bill_code,good_code,quantity,sale_time,PROMOTIONBILL_PLAN_CODE,ACCOUNT_PRICE,retail_price,PERFORM_PRICE
		from "DS_POS"."SALES_ORDERDETAILS" where SALE_TIME>='20181201'		--参数修改处
		)t2
	on t1.sale_bill_code=t2.sale_bill_code
	)t1
	left join
	"DS_POS"."SALES_PRMTNBILL" t2
	on t1.PROMOTIONBILL_PLAN_CODE=t2.PRMTN_BILL_NO
	--limit 10
)t2
on t1.member_id=t2.member_id
and t2.SALE_TIME<=t1.expire_time and t2.SALE_TIME>=t1.BEGIN_TIME
and t1.good_code=t2.good_code
where t1.quantity<=t2.quantity

