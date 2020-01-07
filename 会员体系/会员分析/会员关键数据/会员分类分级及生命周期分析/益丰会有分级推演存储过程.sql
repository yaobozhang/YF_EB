CREATE PROCEDURE EXT_TMP.SP_BOZHANG_MEMB_LEVEL  (IN pro_cate int,IN LV_YEAR VARCHAR(4))
--CALL EXT_TMP.SP_BOZHANG_MEMB_LEVEL(1,20190601)

LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
--------------------------------discribe--------------------------------- 
--实现功能：益丰会员分级推演
--更新时间：需要时跑一下
--版本：1.0
--开发人员 姚泊彰
--开发时间：20190623
-------------------------------------------------------------------------

--------------------------------history----------------------------------
--      时间           开发人员                          目的
-------------------------------------------------------------------------


BEGIN


--程序跑数控制
--如果参数为1，跑第一段程序,得到年转化数据
IF PRO_CATE=1
THEN

---------------------STEP1：得到2018数据并得到等级 START--------------------------------
--step1.1 得到20180601初始化数据
 var_increment11= 
	select member_id
		,SUM(SALE_AMT) AS SALE_AMOUNT		--得到会员一年消费金额
	from "DW"."FACT_SALE_ORDR_DETL" s
	left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
	where s.stsc_date>=ADD_YEARS('2018-06-01',-1)
	and s.stsc_date<'2018-06-01'
	and "ORDR_CATE_CODE"<>'3'		--营运剔除服务性商品
	and "ORDR_CATE_CODE"<>'4'		
	and "PROD_CATE_LEV1_CODE"<>'Y85'	--营运去除品类
	and "PROD_CATE_LEV1_CODE"<>'Y86' 
	and g."GOODS_CODE" <> '8000875'		--营运去除单品
	and g."GOODS_CODE" <> '8000874'
	and s.member_id is not null		--必须是会员
	group by member_id
	having SUM(SALE_AMT)>0
; 

--step1.2 对20180601会员标签进行分级初始化,对于20190601该部分会员为老会员
 var_increment12=
	select t1.customer_id as member_id
		,case when t2.SALE_AMOUNT is null then 0 else floor(t2.SALE_AMOUNT) end as gro_point	--成长值
		,case when t1.create_time<'2017-06-01' then 1 else 2 end as member_cate		--新老会员标识
	from ds_crm.tp_cu_customerbase t1
	left join :var_increment11 t2
	on t1.customer_id=t2.member_id
	where t1.create_time<'2018-06-01'
;

--step1.3 进行级别打标
var_increment10_all=
	select 
	member_id,
	gro_point,
	member_cate,		--1是老，2是新
	case when GRO_POINT=0 then 0
	when GRO_POINT>0    and GRO_POINT<100 then 1
	when GRO_POINT>=100 and GRO_POINT<200 then 2
	when GRO_POINT>=200 and GRO_POINT<400 then 3
	when GRO_POINT>=400 and GRO_POINT<600 then 4
	when GRO_POINT>=600 and GRO_POINT<900 then 5
	when GRO_POINT>=900 and GRO_POINT<1200 then 6
	when GRO_POINT>=1200 and GRO_POINT<1600 then 7
	when GRO_POINT>=1600 and GRO_POINT<2500 then 8
	when GRO_POINT>=2500 and GRO_POINT<4600 then 9
	when GRO_POINT>=4600 and GRO_POINT<7000 then 10
	when GRO_POINT>=7000 and GRO_POINT<10000 then 11
	when GRO_POINT>=10000 then 12 end as LV
	from
	(
		select 
		member_id
		,gro_point
		,member_cate
		from :var_increment12
	)

 ;
---------------------STEP1：得到2019新增数据并关联2018数据得到2019等级 START--------------------------------
--step2.1 得到20190601初始化数据
 var_increment21= 
	select member_id
		,floor(SUM(SALE_AMT)) AS GRO_POINT		--得到会员一年消费金额
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
	and s.member_id is not null		--必须是会员
	group by member_id
	having SUM(SALE_AMT)>0
; 

--step2.2 所有13的数据，即2018初始化的数据在2019都是老会员，直接用该数据关联即可得到老会员在20190601的等级
--那么，我们得到老会员等级如下
var_increment22=
	select member_id,
		GRO_POINT_2018+GRO_POINT_2019_gain as gro_point	--20190601成长值
	from
	(
	select t1.member_id
		,t1.GRO_POINT as GRO_POINT_2018
		,case when t2.GRO_POINT is null then 0 else t2.GRO_POINT end as GRO_POINT_2019_gain	--20190601产生销售
	from :var_increment10_all t1		--20180601初始化时候的数据
	left join :var_increment21 t2
	on t1.member_id=t2.member_id
	)
;

--step2.3 对20190601是老会员的会员进行级别打标
var_increment20_old=
	select 
	member_id,
	gro_point,
	case when GRO_POINT=0 then 0
	when GRO_POINT>0    and GRO_POINT<100 then 1
	when GRO_POINT>=100 and GRO_POINT<200 then 2
	when GRO_POINT>=200 and GRO_POINT<400 then 3
	when GRO_POINT>=400 and GRO_POINT<600 then 4
	when GRO_POINT>=600 and GRO_POINT<900 then 5
	when GRO_POINT>=900 and GRO_POINT<1200 then 6
	when GRO_POINT>=1200 and GRO_POINT<1600 then 7
	when GRO_POINT>=1600 and GRO_POINT<2500 then 8
	when GRO_POINT>=2500 and GRO_POINT<4600 then 9
	when GRO_POINT>=4600 and GRO_POINT<7000 then 10
	when GRO_POINT>=7000 and GRO_POINT<10000 then 11
	when GRO_POINT>=10000 then 12 end as LV
	from
	(
		select 
		member_id
		,gro_point
		from :var_increment22
	)

 ;
 --step2.4 得到2019新会员的成长值数据
var_increment24=
	select member_id
	,GRO_POINT		--得到会员成长值
	from :var_increment21 t1
	where not exists
	(
		select 1 from :var_increment10_all t2
		where t1.member_id=t2.member_id
	)
;

--step2.5 得到2019新会员等级
var_increment20_new=
select 
	member_id,
	gro_point,
	case when GRO_POINT=0 then 0
	when GRO_POINT>0    and GRO_POINT<100 then 1
	when GRO_POINT>=100 and GRO_POINT<200 then 2
	when GRO_POINT>=200 and GRO_POINT<400 then 3
	when GRO_POINT>=400 and GRO_POINT<600 then 4
	when GRO_POINT>=600 and GRO_POINT<900 then 5
	when GRO_POINT>=900 and GRO_POINT<1200 then 6
	when GRO_POINT>=1200 and GRO_POINT<1600 then 7
	when GRO_POINT>=1600 and GRO_POINT<2500 then 8
	when GRO_POINT>=2500 and GRO_POINT<4600 then 9
	when GRO_POINT>=4600 and GRO_POINT<7000 then 10
	when GRO_POINT>=7000 and GRO_POINT<10000 then 11
	when GRO_POINT>=10000 then 12 end as LV
	from
	(
		select 
		member_id
		,gro_point
		from :var_increment24
	)
;

-----------------------step3:分别得到新老会员演化趋势数据----------------------------
--step3.1 得到老会员演化趋势数据
--主要五列字段：上一年等级，下一年等级，上一年等级人数，下一年等级人数，升级占比=下一年升级人数/上一年升级人数
var_increment31=
	select t1.member_id
		,t1.lv as lv_before
		,t2.lv as lv_after
	from :var_increment10_all t1
	left join :var_increment20_old t2
	on t1.member_id=t2.member_id
;
--得到老会员等级演化趋势
var_increment30_old=
	SELECT lv_before
		,lv_after
		,LV_BEFORE_NUM
		,lv_after_NUM
		,lv_after_NUM/LV_BEFORE_NUM AS CHAGE_RATE
	FROM
	(
		SELECT lv_before
			,lv_after
			,MAX(LV_BEFORE_NUM) AS LV_BEFORE_NUM
			,COUNT(1) AS lv_after_NUM
		from 
		(
			select lv_before
				,lv_after
				,COUNT(1) OVER(PARTITION BY lv_before) AS LV_BEFORE_NUM
			from :var_increment31 
		)
		GROUP BY lv_before
			,lv_after
	)
;
--step3.2 得到新会员演化趋势数据
--新会员主要是先得到2018每个等级人数，再得到2019每个等级新客人数，计算增长率
var_increment32=
	select lv as lv_before
		,count(1) as LV_BEFORE_NUM
	from :var_increment10_all t1
	where member_cate=2
	group by lv
	
;
--得到2019新客人数
var_increment33=
	select lv as lv_after
		,count(1) as lv_after_NUM
	from :var_increment20_new t1
	group by lv
;
--关联得到新客数据
var_increment30_new=
	select t1.lv_before
		,t2.lv_after
		,t1.LV_BEFORE_NUM
		,t2.lv_after_NUM
		,t2.lv_after_NUM/t1.LV_BEFORE_NUM as CHAGE_RATE
	from :var_increment32 t1
	left join :var_increment33 t2
	on t1.lv_before=t2.lv_after
;

-----------------------step4:合并新、老客数据并落表----------------------------
var_increment40=
	SELECT lv_before
		,lv_after
		,LV_BEFORE_NUM
		,lv_after_NUM
		,CHAGE_RATE
		,'1' as cate
	FROM :var_increment30_old
	union all
	select lv_before
		,lv_after
		,LV_BEFORE_NUM
		,lv_after_NUM
		,CHAGE_RATE
		,'2' as cate
	FROM :var_increment30_new

;


create column table "EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP1" as
(select * from :var_increment40);

--如果入参为2,跑另一段程序,做年份推演,初始化2019数据
ELSEIF PRO_CATE=2 and LV_YEAR='2019'
THEN

var_increment50=
	select cate,lv_after as lv,sum(lv_after_NUM) as lv_NUM,LV_YEAR as at_year
	from
	"EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP1"
	group by lv_after,cate
;
create column table "EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP2" as
(select * from :var_increment50);


--如果入参为2，并且年份大于2019，使用前一年数据更新当前数据
ELSEIF PRO_CATE=2 and LV_YEAR>'2019'
THEN

--首先，拿到前一年数据
var_increment61=
	select lv,cate,lv_NUM,at_year
	from
	"EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP2"
	where at_year=LV_YEAR-1
;
--然后，今年老会员会变成下面这样
var_increment60_old=
	select lv_after,sum(lv_after_num) as lv_after_num
	from
	(
		SELECT t1.lv,t2.lv_after,t1.lv_NUM*t2.CHAGE_RATE as lv_after_num
		FROM
		(
			select lv,SUM(lv_NUM) AS LV_NUM
			from 
			:var_increment61
			GROUP BY LV
		)t1
		left join
		(
			SELECT lv_before,lv_after,CHAGE_RATE
			FROM 
			"EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP1"
			where cate=1
		)t2
		on t1.lv=t2.lv_before
	)
	group by lv_after
;
--再然后，今年新会员会变成下面这样
var_increment60_new=
	select t1.lv,t1.lv_num*t2.CHAGE_RATE as lv_num,
	from
	(
		select lv,SUM(lv_NUM) AS LV_NUM
		from 
		:var_increment61
		where cate=2
		GROUP BY LV
	)t1
	left join
	(
		SELECT lv_before,CHAGE_RATE
		FROM 
		"EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP1"
		where cate=2
	)t2
	on t1.lv=t2.lv_before
;
--合并新老会员，得到下面的数据
var_increment60=
	select '1' as cate,lv_after as lv,lv_after_num as lv_num,LV_YEAR as at_year
	from
	:var_increment60_old
	UNION ALL
	select '2' as cate,lv,lv_num,LV_YEAR as at_year
	from
	:var_increment60_new
;

--重跑删除
delete from "EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP2" where at_year=lv_year;
--插表
insert into "EXT_TMP"."BOZHANG_MEMB_LEVEL_STEP2"
( 	cate	
	,lv	
	,lv_num		
	,at_year		
)
select  cate	
	,lv	
	,lv_num		
	,at_year
from :VAR_INCREMENT60
;
COMMIT ;

END IF;


 
 end ;
