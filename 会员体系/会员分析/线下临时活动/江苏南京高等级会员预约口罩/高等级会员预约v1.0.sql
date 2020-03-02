--目的：本需求暂时为江苏南京高等级会员口罩预约 
--代码贡献者：姚泊彰
--时间：20200228


--详细需求：1、高端会员答谢预约
--1.1、主消费门店对应城市为江苏南京
--1.2、近两年会员消费>=600
--1.3、近180天有消费  OK
--1.4、过滤掉近30天购买过口罩 
--1.5、过滤掉近55天单次购买金额>600的
--1.6、按照经验值从大到小排序
--2、老带新活动
--2.1、主消费门店对应城市为江苏南京
--2.2、生命周期为忠诚和活跃
--2.3、过滤掉近30天购买过口罩
--2.4、按照经验值从大到小排序


--STEP1：高端会员答谢预约
--1.1、主消费门店对应城市为江苏南京
with t0 as (
	select member_id
		,SALE_AMT
		,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--是否高等级
		,kouzhao_flag --近30天买过口罩
		,day_55_max	--近55天单次购买金额
	from
	(
		SELECT
			t."MEMBER_ID"
			,sum(SALE_AMT) as SALE_AMT
			,max(case when g.GOODS_NAME like '%口罩%' and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --近30天买过口罩
			,max(CASE WHEN stsc_date>=ADD_DAYS('20200228',-55) THEN SALE_AMT END) as day_55_max	--近55天单次购买金额
		FROM dw.fact_sale_ordr_detl t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		and stsc_date>='20180228' and stsc_date<'20200228'
		GROUP BY
			 t."MEMBER_ID"									 --会员编码
	)
)
,
t1 as (
	 select t1.member_id				--会员ID
		,t1.MEMB_LIFE_CYCLE		--生命周期
		,t1.OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,floor(t3.SALE_AMT) as 	SALE_AMT	--成长值
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--主消费门店为江苏南京
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%南京%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--其他会员筛选条件
		(
			select member_id
				,SALE_AMT
			from t0 t2
			where t2.high_level_memb=1
			and t2.kouzhao_flag=0
			and t2.day_55_max<=600
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--最后一次消费时间是180天以内

)
select count(1) from t1


--STEP2、老带新活动
with t0 as (
	select member_id
		,SALE_AMT
		,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--是否高等级
		,kouzhao_flag --近30天买过口罩
	from
	(
		SELECT
			t."MEMBER_ID"
			,sum(SALE_AMT) as SALE_AMT
			,max(case when g.GOODS_NAME like '%口罩%' and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --近30天买过口罩
		FROM dw.fact_sale_ordr_detl t 
		left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
		where t.member_id is not null
		and stsc_date>='20200120' and stsc_date<'20200228'
		GROUP BY
			 t."MEMBER_ID"									 --会员编码
	)
)
,
t1 as (
	 select t1.member_id				--会员ID
		,t1.MEMB_LIFE_CYCLE		--生命周期
		,t1.OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,floor(t3.SALE_AMT) as 	SALE_AMT	--成长值
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--主消费门店为江苏南京
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%南京%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--其他会员筛选条件
		(
			select member_id
				,SALE_AMT
			from t0 t2
			where t2.kouzhao_flag=0
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--最后一次消费时间是180天以内
	and MEMB_LIFE_CYCLE in ('03','04')		

)
select count(1) from t1




----------------------------------------------------------------------------------------------------
--跑不动就落表
--首先落基础表，跟进两年订单得到成长值及近期消费数据
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" as
(
		select member_id
					,SALE_AMT
					,case when SALE_AMT>=600 then 1 else 0 end as high_level_memb	--是否高等级
					,kouzhao_flag --近30天买过口罩
					,day_55_max	--近55天单次购买金额
				from
				(
					SELECT
						t."MEMBER_ID"
						,sum(SALE_AMT) as SALE_AMT
						,max(case when g.GOODS_NAME IS NOT NULL and stsc_date>=ADD_DAYS('20200228',-30) then 1 else 0 end) as kouzhao_flag --近30天买过口罩
						,max(CASE WHEN stsc_date>=ADD_DAYS('20200228',-55) THEN SALE_AMT END) as day_55_max	--近55天单次购买金额
					FROM dw.fact_sale_ordr_detl t 
					left join (
						select GOODS_CODE,GOODS_NAME
						FROM
						dw.DIM_GOODS_cong
						WHERE GOODS_NAME like '%口罩%'
						) g on g.goods_CODE=t.goods_CODE
					where t.member_id is not null
					and stsc_date>='20180228' and stsc_date<'20200228'
					GROUP BY
						 t."MEMBER_ID"									 --会员编码
				)
);
commit;


--然后，得到高端会员口罩预约数据
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228" as
(
	select t1.member_id				--会员ID
		,t1.MEMB_LIFE_CYCLE		--生命周期
		,t1.OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,floor(t3.SALE_AMT) as 	SALE_AMT	--成长值
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--主消费门店为江苏南京
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%南京%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--其他会员筛选条件
		(
			select member_id
				,SALE_AMT
			from "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" t2
			where t2.high_level_memb=1
			and t2.kouzhao_flag=0
			and t2.day_55_max<=600
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--最后一次消费时间是180天以内

)

)

--得到活跃会员数据
create column table "EXT_TMP"."YBZ_MEMB_INFO_20200228_act" as
(
	select t1.member_id				--会员ID
		,t1.MEMB_LIFE_CYCLE		--生命周期
		,t1.OFFLINE_LAST_CNSM_DATE	--最后一次消费时间
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,floor(t3.SALE_AMT) as 	SALE_AMT	--成长值
	from "_SYS_BIC"."YF_BI.EXT_APP.OTHERS/CV_MEMB_CNT_INFO" t1
	--主消费门店为江苏南京
	inner join
		(
			select 
				phmc_code,
				city,
				PHMC_S_NAME
			from dw.dim_phmc
			where city like '%南京%'
		)t2
	on t1.MAIN_CNSM_PHMC_CODE=t2.phmc_code
	inner join 
	--其他会员筛选条件
		(
			select member_id
				,SALE_AMT
			from "EXT_TMP"."YBZ_MEMB_INFO_20200228_step1" t2
			where t2.kouzhao_flag=0
		)t3
	on t1.member_id=t3.member_id
	WHERE OFFLINE_LAST_CNSM_DATE>add_months('20200228',-6)		--最后一次消费时间是180天以内
	and MEMB_LIFE_CYCLE in ('03','04')		
	
)




--高等级会员关联得到会员卡号，姓名，性别，年龄，会员手机
create column table "EXT_TMP"."YBZ_20200228_high" as
(
	select t1.member_id
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,t1.SALE_AMT	--成长值
		,t2.memb_card_code
		,T2.MEMB_NAME			--姓名
		,T2.MEMB_GNDR			--性别
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--年龄
		,T2.MEMB_MOBI				--手机号
	from "EXT_TMP"."YBZ_MEMB_INFO_20200228" t1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
)

--活跃会员关联得到会员卡号，姓名，性别，年龄，会员手机
create column table "EXT_TMP"."YBZ_20200228_act" as
(
	select t1.member_id
		,t1.MAIN_CNSM_PHMC_CODE	--主消费门店
		,t1.MAIN_CNSM_PHMC_NAME	--主消费门店名称
		,t1.SALE_AMT	--成长值
		,t2.memb_card_code
		,T2.MEMB_NAME			--姓名
		,T2.MEMB_GNDR			--性别
		,YEARS_BETWEEN(TO_CHAR(T2.BIRT_date,'YYYY'),'2019') AS AGE 	--年龄
		,T2.MEMB_MOBI				--手机号
	from "EXT_TMP"."YBZ_MEMB_INFO_20200228_act" t1
	left join DW.FACT_MEMBER_BASE  t2 
	on t1.member_id=t2.MEMB_CODE
)