--主题：高等级会员答谢会
--BY：大数据管理部 姚泊彰
--DATE: 20200212

--STEP1：选择目标区域，特定条件下的会员，得到会员的生命周期、主消费区域、最后一次消费时间等信息
with t1 as (
	select member_id				--会员ID
		,MEMB_LIFE_CYCLE		--生命周期
		--,'Act_Place' as memb_place		--分析地点标识
		,OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
		,MAIN_CNSM_PHMC_CODE	--主消费门店
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1 				--ps:这个视图选的是七天内最新的一天
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20191211',-18)		--最后一次消费时间是18个月以内的
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
)

--STEP2：得到这些会员消费商品数据，并得到消费人数前N（N=300）商品，给出人均购买数量及其排名；
--这些商品给到业务方，业务方确认后得到本次活动商品池，导入到系统tmp表中,表名："EXT_TMP"."BOZHANG_Act_Place_GOODS"
--业务完善疾病相关用药数据，确保本次活动商品都有疾病相关用药记录，导入到系统tmp表中 "EXT_TMP"."BOZHANG_DISEASE_UNION_GOOD"
--使用疾病相关用药数据关联本次活动商品池数据，得到疾病可推本次活动商品池数据
/*,
--STEP2.1：得到活动区域会员详细消费数据，需要改消费时间
t2_1 as (
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
			 '20191211'),																	--改消费时间
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180611')) t 																--往前一年半
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
--STEP2.2：得到消费人数前N（N=300）商品
--看每个地方销售人数排名前300商品
t2_2 as (
	select memb_place,GOODS_CODE,GOODS_NAME,memb_num,memb_avg_num
		,row_number() over(partition by memb_place order by memb_num desc) as memb_num_rn
		,row_number() over(partition by memb_place order by memb_avg_num desc) as memb_avg_num_rn
	from
	(
		select memb_place
			,GOODS_CODE
			,GOODS_NAME
			,count(distinct MEMBER_ID) as memb_num
			,sum(SALE_QTY)/count(distinct MEMBER_ID) as memb_avg_num
		from t2_1
		where GOODS_NAME is not null
		AND PROD_CATE_LEV1_CODE!='Y11'	--不要个人护理
		AND  PROD_CATE_LEV1_CODE!='Y13'		--不要日常用品
		AND PROD_CATE_LEV4_NAME!='散装类'
		group by memb_place,GOODS_CODE,GOODS_NAME
	)

)
--给出数据到业务方
select * from t2_2 where memb_num_rn<=300
*/
,
--STEP2.3：使用疾病相关用药数据关联本次活动商品池数据，得到疾病可推本次活动商品池数据
t2_3 as (
	
	SELECT t1.DESEASE_LEV2_CODE		--疾病二级
		,t1.GOODS_CODE				--关联商品编码
		,t1.GOODS_NAME				--关联商品名称
	FROM 
	(
		SELECT DESEASE_LEV2_CODE,GOODS_CODE,max(GOODS_NAME) as GOODS_NAME		--首先，去重，得到疾病关联商品去重后数据
		FROM "EXT_TMP"."BOZHANG_DISEASE_UNION_GOOD"
		GROUP BY DESEASE_LEV2_CODE,GOODS_CODE
	) t1
	inner join 
	(	--选择活动区域的商品
		select GOODS_CODE,Act_Place 
		from "EXT_TMP"."BOZHANG_CHANGSHA_GOODS"
		where Act_Place='changsha'											--此处需要修改为活动区域
	)
	t2		--关联活动商品池
	on  T1.GOODS_CODE=T2.GOODS_CODE
)

--STEP3：根据STEP1中的会员数据得到这些会员的主疾病（疾病消费天数最多的疾病）
--STEP3.1：首先，同人同天算一次
,
t3_1 as (
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
--STEP3.2：得到疾病-药数据
--取出疾病表中只有一条主治用药的单品
,
t3_2 as (
	select t0.GOODS_CODE,t1.DISEASE_LV1_NAME DISEASE_NAME_LEV1
		,t1.DISEASE_LV2_NAME DISEASE_NAME_LEV2 
		,t1.DISEASE_LV2_CODE DISEASE_CODE_LEV2
	from
	(
		select GOODS_CODE 
		from (
			select GOODS_CODE
				,count(1) as num 
			from "DS_COL"."SUCCEZ_GOODS_DISEASE" 
			where EFFECT_SCORE=2 
			group by GOODS_CODE
		)
		where num=1
	)t0
	left join 
	(
		SELECT * FROM "DS_COL"."SUCCEZ_GOODS_DISEASE" where EFFECT_SCORE=2
	)
	t1 
	on t0.GOODS_CODE=t1.GOODS_CODE
) 

--STEP3.3：关联得到每个会员每天买过的疾病情况，并过滤掉疾病为空的天数
,
t3_3 as (
	select MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
		,max(memb_place) as memb_place
	FROM (
		select  t1.MEMBER_ID						--会员编码
			,t1.STSC_DATE  					--销售日期
			,t1.GOODS_CODE    				--商品编码
			,t1.memb_place
			,t2.DISEASE_NAME_LEV1			--疾病一级
			,t2.DISEASE_NAME_LEV2			--疾病二级
			,t2.DISEASE_CODE_LEV2			--疾病二级	
		from t3_1 t1 
		left join t3_2 t2
		on t1.GOODS_CODE=t2.GOODS_CODE
	)
	where DISEASE_NAME_LEV2 is not null
	group by MEMBER_ID
		,STSC_DATE
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
)

--对每个会员数据取疾病购买天数最多疾病
,
t3 as (
	select member_id,memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,rn
	from
	(
		select t1.member_id,t2.memb_place,T2.DISEASE_CODE_LEV2,t2.DISEASE_NAME_LEV2,t2.day_num,row_number() over(partition by T1.member_id order by day_num desc) as rn
		from
		(
			select distinct member_id
			from t3_1
		)t1
		left join 
		(
			select member_id,max(memb_place) as memb_place,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2,count(1) as day_num 
			from t3_3
			group by member_id,DISEASE_CODE_LEV2,DISEASE_NAME_LEV2
		)t2
		ON T1.member_id=T2.member_id
		
	)
	where rn=1
)

--STEP4：根据STEP2和STEP3得到这些会员主疾病关联商品，对每个会员关联到的商品选择买过的，作为主推荐池
,
t4 as (
	SELECT T1.member_id
		,T1.DISEASE_CODE_LEV2
		,T1.DISEASE_NAME_LEV2
		,T1.GOODS_CODE
		,T1.GOODS_NAME
		,5 AS GOODS_LEVEL					--优先级为5
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
		FROM T3 T1
		LEFT JOIN t2_3 T2
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

--STEP5：根据STEP1和STEP2得到这些会员买过的本次活动商品数据，作为补充池
t5 as (
	SELECT MEMBER_ID
		,null as DISEASE_CODE_LEV2
		,null as DISEASE_NAME_LEV2
		,GOODS_CODE
		,max(GOODS_NAME) as GOODS_NAME
		,3 AS GOODS_LEVEL						--优先级为5
		,MAX(STSC_DATE) AS LAST_BUY_DATE		--最后一次购买时间
		,sum(SALE_QTY) as SALE_QTY		--销售数量
		,MAX(ACNT_PRIC) AS MAX_PRICE			--最高成交价
		,MIN(ACNT_PRIC) AS MIN_PRICE			--最低成交价
	FROM T1
	where exists(
		select 1 from
		t2_3 t2
		where t1.goods_code=t2.goods_code
	)
	group by member_id,goods_code
)
,

--STEP6：把STEP4和STEP5中的数据合并，定义数据优先级为购买过的主疾病关联商品(按最近购买时间定优先级)>购买过的本次活动商品（按最近购买时间定优先级）
t6 as (
	select member_id
		,DISEASE_CODE_LEV2
		,DISEASE_NAME_LEV2
		,GOODS_CODE
		,GOODS_NAME
		,GOODS_LEVEL					--优先级为5
		,IF_BUF_FLAG		--是否购买
		,SALE_QTY	--购买数量
		,LAST_BUY_DATE		--最后一次购买时间
		,MAX_PRICE			--最高成交价
		,MIN_PRICE			--最低成交价
		,row_number() over(partition by member_id order by GOODS_LEVEL desc,IF_BUF_FLAG desc,LAST_BUY_DATE desc,SALE_QTY DESC) as rn
	from
	(
		select member_id
			,DISEASE_CODE_LEV2
			,DISEASE_NAME_LEV2
			,GOODS_CODE
			,GOODS_NAME
			,GOODS_LEVEL					--优先级为5
			,IF_BUF_FLAG		--是否购买
			,NULL AS SALE_QTY	--购买数量
			,LAST_BUY_DATE		--最后一次购买时间
			,MAX_PRICE			--最高成交价
			,MIN_PRICE			--最低成交价
		from t4
		union all
		select member_id
			,DISEASE_CODE_LEV2
			,DISEASE_NAME_LEV2
			,GOODS_CODE
			,GOODS_NAME
			,GOODS_LEVEL					--优先级为3
			,IF_BUF_FLAG		--是否购买
			,SALE_QTY
			,LAST_BUY_DATE		--最后一次购买时间
			,MAX_PRICE			--最高成交价
			,MIN_PRICE			--最低成交价
		from t5
	)
	
)
,

--STEP7：每个会员选择一个优先级最高的商品，关联得到会员信息及会员购买商品相关信息得到最终结果
t7 as (
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
	from t6 T1
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
	WHERE T1.rn=1
)

select * from t7






