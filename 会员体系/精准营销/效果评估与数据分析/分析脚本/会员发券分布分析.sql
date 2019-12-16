--开发者：姚泊彰
--问题：分析会员发券分布情况，知道会员过滤程度
--注意点：1、全场用券看用券整单，品类用券看与券品类相同品类商品，单品用券看用券商品
--是否需要修改参数：输入时间需要修改
--开发时间：20190106
--修改记录
--
--step1:拿到用户基础数据
with t1_1 as
(
	SELECT
	 DISTINCT "STSC_DATE" ,--统计日期
	 "MEMB_CARD_CODE",  --会员编号
	 "AT_COMPANY",  --所属公司
	 "AT_PHMC",  --所属门店
	 "MOBILE_NUMBER",  --手机
	 "DIST_AERA",  --片区
	 "IS_NOT_VISIT"  --是否免打扰
FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_MEMBER_BASE_INFO"(PLACEHOLDER."$$endTime$$" => '20190106')
)
,
--step2:打不可打扰标签
t1_2 as
(
	select t1.MEMB_CARD_CODE as member_id
				,case WHEN --t1."TEL_NUMBER" IS NOT NULL AND 		--20181025修改by  ybz
							t1."MOBILE_NUMBER" IS NULL or length(mobile_number)!=11 THEN '1'           --不可干预会员,20181025ybz新增判断是否11位
							 WHEN t1."IS_NOT_VISIT" = '1' THEN '2'            --短信免打扰会员  	
							 WHEN t1."AT_COMPANY" = '1008' THEN '3'           --粤海会员打标为5  	
							 WHEN t4."STORE_CODE" IS NOT NULL THEN '4'       --徐州南京门店会员                --20180125.n
	 						 WHEN t5.state!='EFC' THEN '5'  --状态不正常会员
							 when t6.DIST_CODE='10013530' THEN '6' --灌云社区过滤
							 --WHEN T3.STORE IS NOT NULL AND t1."IS_NOT_VISIT" = '0' AND T1."MOBILE_NUMBER" IS NOT NULL THEN '5'   --慢病门店会员 	
							 ELSE '0'   ---B方案可分流会员
							 end as member_type_flag
				,case WHEN --t1."TEL_NUMBER" IS NOT NULL AND 		--20181025修改by  ybz
							t1."MOBILE_NUMBER" IS NULL or length(mobile_number)!=11 THEN 'no_coupon'           --不可干预会员
							 WHEN t1."IS_NOT_VISIT" = '1' THEN 'no_coupon'            --短信免打扰会员  	
							 WHEN t1."AT_COMPANY" = '1008' THEN 'no_coupon'           --粤海会员打标为5  	
							 WHEN t4."STORE_CODE" IS NOT NULL THEN 'no_coupon'       --徐州南京门店会员                --20180125.n
							 WHEN t5.state!='EFC' THEN 'no_coupon'  --状态不正常会员
							 when t6.DIST_CODE='10013530' THEN 'no_coupon' --灌云社区过滤
							 --WHEN T3.STORE IS NOT NULL AND t1."IS_NOT_VISIT" = '0' AND T1."MOBILE_NUMBER" IS NOT NULL THEN 'no_coupon'   --慢病门店会员 	
							 ELSE 'coupon' end as member_coupon_flag				 						 
			from 
				t1_1 t1
					LEFT JOIN dw.bi_chronic_store t3 
					  ON t3.STORE=t1.AT_PHMC                                     
					LEFT JOIN "DW"."NANJING_XUZHOU_STORE_COFIG" t4              
					  ON  t4.STORE_CODE =t1.AT_PHMC 
					LEFT JOIN "DW"."FACT_MEMBER_BASE" t5
					on t1.MEMB_CARD_CODE =t5.memb_code
					left join "DW"."DIM_PHMC" t6
					on t1.AT_PHMC=t6.PHMC_CODE
	
)
,
--step3:得到用于统计的所有标签
t1 as
(
	select t1.member_id,t2.member_coupon_flag,t1.AB_TEST_REDUCE,t1.member_type,t2.member_type_flag,t1.coupon_flag_r,T1.AB_TEST_FLAG
	from 
	dm.user_coupon_abtest_label_L1 t1
	inner join t1_2 T2
	on t1.member_id=t2.member_id
	where t1.end_date='99991231'
)
,
--step4:得到所有统计数据
t2 as
(
	--得到总数
	select '总数' as name,num from (select count(1) as num from t1)
	union all
	select '政策过滤人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='no_coupon')
	union all
	select '业务过滤人数' as name,num from (select count(1) as num from t1 where AB_TEST_REDUCE=0)
	union all
	select '政策过滤手机号异常人数' as name,num from (select count(1) as num from t1 where member_type_flag=1)
	union all
	select '政策过滤短信免打扰人数' as name,num from (select count(1) as num from t1 where member_type_flag=2)
	union all
	select '政策过滤粤海会员人数' as name,num from (select count(1) as num from t1 where member_type_flag=3)
	union all
	select '政策过滤徐州南京门店人数' as name,num from (select count(1) as num from t1 where member_type_flag=4)
	union all
	select '政策过滤状态不正常人数' as name,num from (select count(1) as num from t1 where member_type_flag=5)
	union all
	select '政策过滤灌云社区人数' as name,num from (select count(1) as num from t1 where member_type_flag=6)
	union all
	select '业务过滤老客人数' as name,0 as num from dummy
	union all
	select '业务过滤流失人数' as name,0 as num from dummy
	union all
	select '业务过滤新客人数' as name,num from (select count(1) as num from t1 where AB_TEST_REDUCE=0)
	union all
	select '政策、业务过滤后总人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1)
	union all
	select '政策、业务过滤后老客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='RE_PURCH')
	union all
	select '政策、业务过滤后流失人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='WAKE_UP')
	union all
	select '政策、业务过滤后新客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r IN ('NEW'))
	union all
	select '政策、业务过滤后实验组老客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='RE_PURCH' AND AB_TEST_FLAG LIKE '%EXP%')
	union all
	select '政策、业务过滤后实验组流失人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='WAKE_UP' AND AB_TEST_FLAG LIKE '%EXP%')
	union all
	select '政策、业务过滤后实验组新客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r IN ('NEW') AND AB_TEST_FLAG LIKE '%EXP%')
	union all
	select '政策、业务过滤后默认组老客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='RE_PURCH' AND AB_TEST_FLAG LIKE '%DEF%')
	union all                                                
	select '政策、业务过滤后默认组流失人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r='WAKE_UP' AND AB_TEST_FLAG LIKE '%DEF%')
	union all                                                
	select '政策、业务过滤后默认组新客人数' as name,num from (select count(1) as num from t1 where member_coupon_flag='coupon' and AB_TEST_REDUCE=1 and coupon_flag_r IN ('NEW') AND AB_TEST_FLAG LIKE '%DEF%')

)
SELECT * FROM T2


-------------------------------------------------------------------------------单维度分析-------------------------------------------------------------
With t1 as(
			select MEMB_CODE
		,MEMB_SOUR --会员来源
		,APPLY_TYPE
		,case when MEMB_MOBI not  like_regexpr --'^1[3578]\d{9}$'
		'^(((13[0-9])|(14[579])|(15([0-3]|[5-9]))|(16[6])|(17[0135678])|(18[0-9])|(19[89]))\d{8})$' then 1 else 0 end as flag1
		,case when STATE ='CNL' then 1 else 0 end as flag2
		,case when IS_NO_DIST!='1'  then 1 else 0 end as flag3
		from  DW.FACT_MEMBER_BASE 
)

select '手机号异常',count(1) as num from t1 where flag1=1
union all
select '状态为注销',count(1) as num from t1 where flag2=1
union all
select '不可打扰',count(1) as num from t1 where flag3=1
union all
select '手机号异常并且状态注销',count(1) as num from t1 where flag1=1 and flag2=1
union all
select '手机号异常并且不可打扰',count(1) as num from t1 where flag1=1 and flag3=1
union all
select '状态注销并且不可打扰',count(1) as num from t1 where flag2=1 and flag3=1
union all
select '手机号异常并且状态注销并且不可打扰',count(1) as num from t1 where flag1=1 and flag2=1 and flag3=1
union all
select '手机号异常或者状态注销或者不可打扰',count(1) as num from t1 where flag1=1 or flag2=1 or flag3=1
