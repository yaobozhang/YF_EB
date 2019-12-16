--主题：流失高等级会员答谢会
--BY：大数据管理部 姚泊彰
--DATE: 20191213

--STEP1：得到目标会员并落表
--会员标签时间20191211
drop table "EXT_TMP"."YBZ_MEMB_INFO_20191213";
create column table "EXT_TMP"."YBZ_MEMB_INFO_20191213" as
(
        select member_id				--会员ID
				,MEMB_LIFE_CYCLE		--生命周期
				,case when MAIN_CNSM_DEPRT_CODE='10011441' then 'changsha'													--分析地点为长沙和宁乡
				else 'ningxiang' end as memb_place		--会员主消费地方
				,OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
				,MAIN_CNSM_PHMC_CODE	--主消费门店
		from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
		WHERE OFFLINE_LAST_CNSM_DATE>add_months('20191211',-18)		--最后一次消费时间是180天以内
		and MEMB_LIFE_CYCLE in ('06','07')																					--选择生命周期
		and (																												--选择活动地点
				MAIN_CNSM_DEPRT_CODE ='10011441' 	--长沙门管部
				OR 
				MAIN_CNSM_DIST_CODE IN ('10012924','10017063') --宁乡一区和宁乡二区
			)
		and exists(																											--选择消费条件
			select 1 from (
				select MEMBER_ID
					,sum(SALE_AMT) as SALE_AMT
				from "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
					'20191211'),	--会员一年半内消费
					'PLACEHOLDER' = ('$$BeginTime$$',
					'20180611'))
				group by MEMBER_ID
			)t2 
			where t1.member_id=t2.member_id
			and SALE_AMT >= 400
		)
);
commit;

--STEP2:得到会员推荐或消费商品信息
--把会员池进行封装，可以统一修改源
with t1_1 as (
	select member_id
		,memb_life_cycle
		,memb_place
		,OFFLINE_LAST_CNSM_DATE			
		,MAIN_CNSM_PHMC_CODE
	from "EXT_TMP"."YBZ_MEMB_INFO_20191213"
	where memb_place='changsha'								--选择哪个地方的人
)
,
--得到不同地点的商品池数据
t1_2 as (
	
	SELECT t1.DESEASE_LEV2_CODE		--疾病二级
		,t1.GOODS_CODE				--关联商品
		,t1.GOODS_NAME				
		,case when t2.GOODS_CODE is not null then 1 else 0 end as changsha_flag		--是否长沙商品池
		,case when t2.GOODS_CODE is not null then 1 else 0 end as ningxiang_flag	--是否宁乡商品池
	FROM 
	(
	SELECT DESEASE_LEV2_CODE,GOODS_CODE,max(GOODS_NAME) as GOODS_NAME		--首先，去重，得到疾病关联商品去重后数据
	FROM "EXT_TMP"."BOZHANG_DISEASE_UNION_GOOD"
	GROUP BY DESEASE_LEV2_CODE,GOODS_CODE
	) t1
	left join "EXT_TMP"."BOZHANG_CHANGSHA_GOODS" t2		--关联长沙商品池
	on  T1.GOODS_CODE=T2.GOODS_CODE
	left join "EXT_TMP"."BOZHANG_NINGXIANG_GOODS" t3		--关联宁乡商品池
	on T1.GOODS_CODE=T3.GOODS_CODE
)
,
t1_3 as (
	SELECT DESEASE_LEV2_CODE
		,GOODS_CODE
		,GOODS_NAME
	FROM T1_2
	WHERE changsha_flag=1								--选择哪个地方的商品池

)
,
--得到商品消费数据，按消费人数排序
t1 as (
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",								--会员编码
			 t."GOODS_CODE",    							--商品编码
			g.PROD_CATE_LEV1_CODE,
			g.PROD_CATE_LEV4_CODE,
			g.PROD_CATE_LEV4_NAME,
			g.GOODS_NAME,			--商品名称
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT",				--销售额
			 sum("SALE_QTY") AS "SALE_QTY",				--销售数量
			MAX(ACNT_PRIC) AS ACNT_PRIC		--商品单价
			,t1.memb_place
			,t1.OFFLINE_LAST_CNSM_DATE			
			,t1.MAIN_CNSM_PHMC_CODE
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191211'),				--当前会员标签
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180611')) t 			--往前一年半
		inner join t1_1 t1
		on t.MEMBER_ID=t1.MEMBER_ID
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid		--使用left join，防止商品不存在
		GROUP BY t."STSC_DATE"                                  --销售日期
			,t."PHMC_CODE"                                  --门店编码
			,t."MEMBER_ID"									 --会员编码
			,t."GOODS_CODE"   							--商品编码
			,g.PROD_CATE_LEV4_CODE
			,g.PROD_CATE_LEV4_NAME
			,g.GOODS_NAME			--商品名称
			,g.PROD_CATE_LEV1_CODE
			,t1.memb_place
			,t1.OFFLINE_LAST_CNSM_DATE			
			,t1.MAIN_CNSM_PHMC_CODE

)

,
--看每个地方销售人数排名前300商品
t2 as (
	select memb_place,GOODS_CODE,GOODS_NAME,memb_num
		,row_number() over(partition by memb_place order by memb_num desc) as rn
	from
	(
		select memb_place,GOODS_CODE,GOODS_NAME,count(distinct MEMBER_ID) as memb_num
		from t1
		where GOODS_NAME is not null
		AND PROD_CATE_LEV1_CODE!='Y11'	--不要个人护理
		AND  PROD_CATE_LEV1_CODE!='Y13'		--不要日常用品
		AND PROD_CATE_LEV4_NAME!='散装类'
		group by memb_place,GOODS_CODE,GOODS_NAME
	)

)
,
--看每个地方人均购买数量排名前300商品
t3 as (
	select memb_place,GOODS_CODE,GOODS_NAME,memb_avg_num,memb_num
		,row_number() over(partition by memb_place order by memb_avg_num desc) as rn
	from
	(
		select memb_place,GOODS_CODE,GOODS_NAME,sum(SALE_QTY)/count(distinct MEMBER_ID) as memb_avg_num,count(distinct MEMBER_ID) as memb_num
		from t1
		where GOODS_NAME is not null
		AND PROD_CATE_LEV1_CODE!='Y11'
		AND  PROD_CATE_LEV1_CODE!='Y13'
		AND PROD_CATE_LEV4_NAME!='散装类'
		group by memb_place,GOODS_CODE,GOODS_NAME
	)

)
,
--STEP4:得到会员疾病标签
--首先，同人同天算一次
t4_1 as (
	SELECT STSC_DATE,
		GOODS_CODE,
		MEMBER_ID,
		max(memb_place) as memb_place,		--会员地点
		SUM(SALE_AMT) AS SALE_AMT,	--销售额
		SUM(GROS_PROF_AMT) AS GROS_PROF_AMT
	FROM t1
	GROUP BY STSC_DATE,
		GOODS_CODE,
		MEMBER_ID
	
) 
--得到疾病-药数据
--首先得到主治用药，并得到每个药的条数
,
t4_2 as (
	select GOODS_CODE,count(1) as num from "DS_COL"."SUCCEZ_GOODS_DISEASE" where EFFECT_SCORE=2 group by GOODS_CODE
)
--然后，取出疾病表中只有一条主治用药的单品
,
t4_3 as (
	select t0.GOODS_CODE,t1.DISEASE_LV1_NAME DISEASE_NAME_LEV1
		,t1.DISEASE_LV2_NAME DISEASE_NAME_LEV2 
		,t1.DISEASE_LV2_CODE DISEASE_CODE_LEV2
	from
	(
		select GOODS_CODE from t4_2 where num=1
	)t0
	left join 
	(
		SELECT * FROM "DS_COL"."SUCCEZ_GOODS_DISEASE" where EFFECT_SCORE=2
	)
	t1 
	on t0.GOODS_CODE=t1.GOODS_CODE
) 
,
--关联得到每个会员每天买过的疾病情况，并过滤掉疾病为空的天数
t4_4 as (
	select  t1.MEMBER_ID,						--会员编码
		 t1.STSC_DATE,  					--销售日期
		 t1.GOODS_CODE,    				--商品编码
		 t1.memb_place,
		 t2.DISEASE_NAME_LEV1,			--疾病一级
		 t2.DISEASE_NAME_LEV2			--疾病二级
		,t2.DISEASE_CODE_LEV2			--疾病二级	
	from t4_1 t1 
	left join t4_3 t2
	on t1.GOODS_CODE=t2.GOODS_CODE

)
,
t4_5 as (
	select MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
		,max(memb_place) as memb_place
	FROM t4_4
	where DISEASE_NAME_LEV2 is not null
	group by MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
)

--step4:对每个会员数据取疾病购买天数最多疾病
,
t4 as (
	select member_id,memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,rn
	from
	(
		select t1.member_id,t2.memb_place,T2.DISEASE_CODE_LEV2,t2.DISEASE_NAME_LEV2,t2.day_num,row_number() over(partition by T1.member_id order by day_num desc) as rn
		from
		(
			select distinct member_id
			from t4_4
		)t1
		left join 
		(
			select member_id,max(memb_place) as memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,count(1) as day_num 
			from t4_5
			group by member_id,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2
		)t2
		ON T1.member_id=T2.member_id
		
	)
	where rn=1
)
,
--得到会员疾病关联商品，主要选买过的
t5_1 as (
	SELECT T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,CASE WHEN T2.GOODS_CODE IS NOT NULL THEN 1 ELSE 0 END AS IF_BUF_FLAG
		,T2.LAST_BUY_DATE		--最后一次购买时间
		,T2.MAX_PRICE			--最高成交价
		,T2.MIN_PRICE			--最低成交价
	FROM
	(
		SELECT T1.member_id			--会员ID
			,memb_place				--会员主消费区域
			,T1.DISEASE_CODE_LEV2	--会员疾病
			,T1.DISEASE_NAME_LEV2
			,T2.GOODS_CODE			--会员疾病关联商品
			,T2.GOODS_NAME
		FROM T4 T1
		LEFT JOIN t1_3 T2
		ON T1.DISEASE_CODE_LEV2=T2.DESEASE_LEV2_CODE
	)T1
	LEFT JOIN 
	(
		SELECT MEMBER_ID
			,GOODS_CODE
			,MAX(STSC_DATE) AS LAST_BUY_DATE		--最后一次购买时间
			,MAX(ACNT_PRIC) AS MAX_PRICE			--最高成交价
			,MIN(ACNT_PRIC) AS MIN_PRICE			--最低成交价
		FROM T1 
		GROUP BY MEMBER_ID,GOODS_CODE
	)T2
	ON T1.MEMBER_ID=T2.MEMBER_ID 
	AND T1.GOODS_CODE=T2.GOODS_CODE
)
,
--找到每个会员通过疾病可以推荐的商品
T5 AS (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,t1.IF_BUF_FLAG
		,T1.LAST_BUY_DATE		--最后一次购买时间
		,T1.MAX_PRICE			--最高成交价
		,T1.MIN_PRICE			--最低成交价
	from
	(
		SELECT T1.member_id
			,T1.DISEASE_CODE_LEV2
			,T1.DISEASE_NAME_LEV2
			,T1.GOODS_CODE
			,T1.GOODS_NAME
			,t1.IF_BUF_FLAG
			,T1.LAST_BUY_DATE		--最后一次购买时间
			,T1.MAX_PRICE			--最高成交价
			,T1.MIN_PRICE			--最低成交价
			,row_number() over(partition by member_id order by IF_BUF_FLAG desc,GOODS_CODE asc) as rn
		FROM T5_1 t1
	)t1
	where rn=1
)
,

--得到每个会员基本信息
t6 as (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,t1.IF_BUF_FLAG
		,T1.LAST_BUY_DATE		--最后一次购买时间
		,T1.MAX_PRICE			--最高成交价
		,T1.MIN_PRICE			--最低成交价
		,T2.MEMB_NAME			--姓名
		,T2.MEMB_GNDR			--性别
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--年龄
		,T2.MEMB_MOBI				--手机号
		,T2.BELONG_PHMC_CODE		--所属门店
		,t3.memb_place				--主消费区域
		,t3.OFFLINE_LAST_CNSM_DATE			--最后一次消费时间
		,t3.MAIN_CNSM_PHMC_CODE			--主消费门店
		,t4.sale_amt
		,t4.sale_times
		,T2.MEMB_CARD_CODE
	from t5 T1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
	left join t1_1 t3
	on t1.member_id=t3.member_id
	left join (
		select member_id
			,sum(sale_amt) as sale_amt
			,count(1) as sale_times
		from t1
		group by member_id
	)t4
	on t1.member_id=t4.member_id
)
,
--
t7_1 as (
	select MEMBER_ID
			,GOODS_CODE
			,GOODS_NAME
			,LAST_BUY_DATE		--最后一次购买时间
			,MAX_PRICE			--最高成交价
			,MIN_PRICE			--最低成交价
		from
		(
			select MEMBER_ID
				,GOODS_CODE
				,GOODS_NAME
				,LAST_BUY_DATE		--最后一次购买时间
				,MAX_PRICE			--最高成交价
				,MIN_PRICE			--最低成交价
				,row_number() over(partition by member_id order by SALE_QTY desc) as rn
			from
			(
				SELECT MEMBER_ID
					,GOODS_CODE
					,max(GOODS_NAME) as GOODS_NAME
					,MAX(STSC_DATE) AS LAST_BUY_DATE		--最后一次购买时间
					,sum(SALE_QTY) as SALE_QTY		--销售数量
					,MAX(ACNT_PRIC) AS MAX_PRICE			--最高成交价
					,MIN(ACNT_PRIC) AS MIN_PRICE			--最低成交价
				FROM T1
				where exists(
					select 1 from
					t1_3 t2
					where t1.goods_code=t2.goods_code
				)
				group by member_id,goods_code
			)
		)where rn=1
)
,
--如果没匹配到药品，用每个人活动商品中买得最多的作为补充
t7 as (
	select T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,case when T1.GOODS_CODE is not null then T1.GOODS_CODE else t2.GOODS_CODE end as GOODS_CODE
		,case when T1.GOODS_CODE is not null then T1.GOODS_NAME else t2.GOODS_NAME end as GOODS_NAME
		,t1.IF_BUF_FLAG
		,case when T1.GOODS_CODE is not null then T1.LAST_BUY_DATE else T2.LAST_BUY_DATE end as LAST_BUY_DATE		--最后一次购买时间
		,case when T1.GOODS_CODE is not null then T1.MAX_PRICE else T2.MAX_PRICE end as MAX_PRICE 			--最高成交价
		,case when T1.GOODS_CODE is not null then T1.MIN_PRICE else T2.MIN_PRICE end as MIN_PRICE 			--最低成交价
		,T1.MEMB_NAME			--姓名
		,T1.MEMB_GNDR			--性别
		,t1.AGE 	--年龄
		,T1.MEMB_MOBI				--手机号
		,T1.BELONG_PHMC_CODE		--所属门店
		,t1.memb_place				--主消费区域
		,t1.OFFLINE_LAST_CNSM_DATE			--最后一次消费时间
		,t1.MAIN_CNSM_PHMC_CODE			--主消费门店
		,t1.sale_amt
		,t1.sale_times
		,T1.MEMB_CARD_CODE
	from t6 t1
	left join t7_1 t2
	on t1.member_id=t2.member_id
)
select * from t7














