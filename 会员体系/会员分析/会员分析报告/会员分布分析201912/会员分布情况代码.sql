--会员分布情况
--代码贡献者：姚泊彰
--代码更新时间：20191128
--数据口径：见各自模块


--简介：会员分布情况总共分为3块：1、生命周期分布；2、年龄性别分布；3、等级分布

--0、数据准备
	--0.1、会员分析(20191128-20191128)：订单数据基础过滤（积分兑换订单及订金订单、服务性商品及行政赠品、塑料袋）
	With t0_1 as(
		SELECT
			 t."STSC_DATE",  								--销售日期
			 t."PHMC_CODE",     							--门店编码
			 t."MEMBER_ID",								--会员编码
			 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
			 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
			 sum("SALE_AMT") AS "SALE_AMT"				--销售额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191127'),
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20171127')) t 
		GROUP BY t."STSC_DATE",                                  --销售日期
			 t."PHMC_CODE",                                  --门店编码
			 t."MEMBER_ID"									 --会员编码
	)
	,
	--得到每个会员消费情况
	t0_2 as (
		select "MEMBER_ID" as MEMBER_ID,								--会员编码
			 sum("GROS_PROF_AMT") AS GROS_PROF_AMT,		--毛利额
			 sum("SALE_AMT") AS SALE_AMT,				--年产值
			 count(1) as sale_num					--消费次数
		from t0_1
		where is_member=1
		group by "MEMBER_ID"
	)
	,
	--关联得到会员属性信息
	t0 as (
		select t1.member_id				--会员
			,t1.MEMB_GNDR				--性别
			,t1.age						--年龄
			,t1.ADMS_ORG_CODE			--分公司编码
			,t2.MEMB_LIFE_CYCLE			--生命周期
			,t3.GRADE_ID				--等级
		from 
		(
			select t1.MEMB_CODE member_id,		--会员
				t1.OPEN1_PHMC_CODE PHMC_CODE,	--开卡门店
				CASE WHEN t1.MEMB_GNDR in ('男','女') then t1.MEMB_GNDR else '不明' end as MEMB_GNDR,				--性别
				case when floor(years_between(t1.BIRT_DATE,now()))>=20
					and  floor(years_between(t1.BIRT_DATE,now()))<=85 then  floor(years_between(t1.BIRT_DATE,now())/5)*5
				else  '00'  end as age,			--年龄段
				t2.ADMS_ORG_CODE				--开卡公司
			from "DW"."FACT_MEMBER_BASE" t1
			left join 
			(
				select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				phmc_type
				from dw.dim_phmc
			) t2
			on t1.OPEN1_PHMC_CODE=t2.phmc_code
		)t1
		left join 
		(
			select member_id
				,MEMB_LIFE_CYCLE 
			from "DM"."FACT_MEMBER_CNT_INFO"
			where data_date='20191126'		--会员标签日期
		)t2 
		on t1.member_id=t2.member_id
		left join
		(
			select customer_id member_id	--会员ID
			,MAX(GRADE_ID) AS GRADE_ID		--等级
			from "DS_ZT"."SCRM_MEMBER"	
			group by customer_id
		)t3
		on t1.member_id=t3.member_id
		where t1.ADMS_ORG_CODE='1002'		--江苏
	),
	
--1、会员分布分析
		--1.0 得到江苏会员每个人的标签及消费情况
		t1_0 as (
			select t1.member_id				--会员
			,t1.MEMB_GNDR				--性别
			,t1.age						--年龄
			,t1.ADMS_ORG_CODE			--分公司编码
			,t1.MEMB_LIFE_CYCLE			--生命周期
			,t1.GRADE_ID				--等级
			,case when MEMB_LIFE_CYCLE=01 then 0 else t2.SALE_AMT end as SALE_AMT			--年产值
			,t2.sale_num				--销售次数
			,t2.GROS_PROF_AMT			--年毛利
			from t0 t1
			left join t0_2 t2
			on t1.member_id=t2.member_id
			where t1.MEMB_LIFE_CYCLE is not null			--生命周期
		)
		
		
		
		
		
		
		
		
		--正常运行代码
		with t1_0 as (
			select * from "EXT_TMP"."YBZ_MEMB_INFO_20191128"
			where ADMS_ORG_CODE='1002'
		)
		,
		--1.1 得到江苏会员分布情况
		--得到会员整体消费情况
		t1_1 as (
			select count(member_id) as memb_num	--会员人数
				,count	(case when SALE_AMT>0 then member_id 		end) as	memb_num_buy        --近一年有消费会员人数
				,avg	(case when SALE_AMT>0 then SALE_AMT 		end) as	SALE_AMT 	            --平均年产值
				,avg	(case when SALE_AMT>0 then GROS_PROF_AMT 	end) as	GROS_PROF_AMT             --平均年毛利额
				,avg	(case when SALE_AMT>0 then sale_num 		end) as	sale_num 	             --平均年消费次数
			from t1_0
		)
		,
		--1.2 得到江苏会员生命周期分布情况
		t1_2 as (
			select MEMB_LIFE_CYCLE	--生命周期
				,count(member_id) as memb_num	--会员人数
				,count	(case when SALE_AMT>0 then member_id 		end) as	memb_num_buy        --近一年有消费会员人数
				,avg	(case when SALE_AMT>0 then SALE_AMT 		end) as	SALE_AMT 	            --平均年产值
				,avg	(case when SALE_AMT>0 then GROS_PROF_AMT 	end) as	GROS_PROF_AMT             --平均年毛利额
				,avg	(case when SALE_AMT>0 then sale_num 		end) as	sale_num 	             --平均年消费次数
			from t1_0
			group by MEMB_LIFE_CYCLE
		)
		,
		--1.3 得到江苏会员年龄性别分布情况
		t1_3 as (
			select MEMB_GNDR	--性别
				,age			--年龄
				,count(member_id) as memb_num	--会员人数
				,count	(case when SALE_AMT>0 then member_id 		end) as	memb_num_buy        --近一年有消费会员人数
				,avg	(case when SALE_AMT>0 then SALE_AMT 		end) as	SALE_AMT 	            --平均年产值
				,avg	(case when SALE_AMT>0 then GROS_PROF_AMT 	end) as	GROS_PROF_AMT            --平均年毛利额
				,avg	(case when SALE_AMT>0 then sale_num 		end) as	sale_num 	            --平均年消费次数
			from t1_0
			group by MEMB_GNDR	--性别
				,age			--年龄
		)
		,
		--1.4 得到江苏会员等级分布情况
		t1_4 as (
			select GRADE_ID	--生命周期
				,count(member_id) as memb_num	--会员人数
				,count	(case when SALE_AMT>0 then member_id 		end) as	memb_num_buy        --近一年有消费会员人数
				,avg	(case when SALE_AMT>0 then SALE_AMT 		end) as	SALE_AMT 	            --平均年产值
				,avg	(case when SALE_AMT>0 then GROS_PROF_AMT 	end) as	GROS_PROF_AMT             --平均年毛利额
				,avg	(case when SALE_AMT>0 then sale_num 		end) as	sale_num 	             --平均年消费次数
			from t1_0
			group by GRADE_ID
		)
		select * from t1_1

		
		
		
		
		
		
		
		
		
		
		
--源数据落表
create column table "EXT_TMP"."YBZ_MEMB_INFO_20191128" as
(select * from (
	select t1.member_id				--会员
			,t1.MEMB_GNDR				--性别
			,t1.age						--年龄
			,t1.ADMS_ORG_CODE			--分公司编码
			,t1.MEMB_LIFE_CYCLE			--生命周期
			,t1.GRADE_ID				--等级
			,case when MEMB_LIFE_CYCLE=01 then 0 else t2.SALE_AMT end as SALE_AMT			--年产值
			,t2.sale_num				--销售次数
			,t2.GROS_PROF_AMT			--年毛利
			from (
				select t1.member_id				--会员
			,t1.MEMB_GNDR				--性别
			,t1.age						--年龄
			,t1.ADMS_ORG_CODE			--分公司编码
			,t2.MEMB_LIFE_CYCLE			--生命周期
			,t3.GRADE_ID				--等级
		from 
		(
			select t1.MEMB_CODE member_id,		--会员
				t1.OPEN1_PHMC_CODE PHMC_CODE,	--开卡门店
				CASE WHEN t1.MEMB_GNDR in ('男','女') then t1.MEMB_GNDR else '不明' end as MEMB_GNDR,				--性别
				case when floor(years_between(t1.BIRT_DATE,now()))>=20
					and  floor(years_between(t1.BIRT_DATE,now()))<=85 then  floor(years_between(t1.BIRT_DATE,now())/5)*5
				else  '00'  end as age,			--年龄段
				t2.ADMS_ORG_CODE				--开卡公司
			from "DW"."FACT_MEMBER_BASE" t1
			left join 
			(
				select 
				phmc_code,
				ADMS_ORG_CODE,
				ADMS_ORG_NAME,
				phmc_type
				from dw.dim_phmc
			) t2
			on t1.OPEN1_PHMC_CODE=t2.phmc_code
			)t1
			left join 
			(
				select member_id
					,MEMB_LIFE_CYCLE 
				from "DM"."FACT_MEMBER_CNT_INFO"
				where data_date='20191126'		--会员标签日期
			)t2 
			on t1.member_id=t2.member_id
			left join
			(
				select customer_id member_id	--会员ID
				,MAX(GRADE_ID) AS GRADE_ID		--等级
				from "DS_ZT"."SCRM_MEMBER"	
				group by customer_id
			)t3
			on t1.member_id=t3.member_id
			--where t1.ADMS_ORG_CODE='1002'		--江苏
			
			) t1
			left join (
				select "MEMBER_ID" as MEMBER_ID,								--会员编码
					 sum("GROS_PROF_AMT") AS GROS_PROF_AMT,		--毛利额
					 sum("SALE_AMT") AS SALE_AMT,				--年产值
					 count(1) as sale_num					--消费次数
					from (
						SELECT
							 t."STSC_DATE",  								--销售日期
							 t."PHMC_CODE",     							--门店编码
							 t."MEMBER_ID",								--会员编码
							 case when t.member_id is not null then 1 else 0 end as is_member,        				 --是否是会员
							 sum("GROS_PROF_AMT") AS "GROS_PROF_AMT",	--毛利额
							 sum("SALE_AMT") AS "SALE_AMT"				--销售额
						FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
							 '20191127'),
							 'PLACEHOLDER' = ('$$BeginTime$$',
							 '20171127')) t 
						GROUP BY t."STSC_DATE",                                  --销售日期
							 t."PHMC_CODE",                                  --门店编码
							 t."MEMBER_ID"									 --会员编码
					)
					where is_member=1
					group by "MEMBER_ID"
			
			) t2
			on t1.member_id=t2.member_id
			where t1.MEMB_LIFE_CYCLE is not null			--生命周期

));						--单品推送
commit;







		