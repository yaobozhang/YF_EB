--会员权益取数脚本
---------------------取各层会员近12个月销售额等数据--------------------------------
select sum(sale_money_recnt)
	,sum(M_Recnt)
	,sum(F_Recnt)
	,sum(SALE_POINTS)
	,sum(Point_12)
from
( 
	select t1.member_id    --会员id
		,t1.sale_money_recnt   --近12个月销售额
		,t1.M_Recnt			--近12个月毛利额
		,t1.F_Recnt	--近12个月消费频次
		,t2.SALE_POINTS  --累积积分
		,t2.Point_12		--近12个月积分
		,t1.VALUE_LEVEL		--会员等级
	from "EXT_TMP"."MEMB_VALUE_MODEL_RESULT" t1
	left join 
	(
		select t1.customer_id member_id,t1.Point_12,t2.SALE_POINTS
		from
		(
			select customer_id,sum(case when points>0 then points else 0 end) as Point_12
			from "DS_CRM"."TP_MEMBERPOINTCERTILOGS" 
			where at_time>=add_years('20181115',-1)
			group by customer_id
		) t1 
		inner join "DS_CRM"."TP_CU_CUSTOMERINFO" t2
		on t1.customer_id=t2.customer_id
	) t2
)
GROUP BY VALUE_LEVEL




--------------------------------------------------------------------------------
