--首先，得到每个分公司、每个门管部、每个片区、每个门店的线上线下销售情况

with t1 as
(
	select adms_org_name
		--,city
		--,phmc_code
		--,phmc_s_name
		,year_month
		,sum(sale_total) sale_total
		,sum(case when line_flag='on' then  sale_total end ) on_line_sale
		,sum(case when line_flag='off' then  sale_total end) off_line_sale
		--,sum(case when "线上/线下"='线上' then  "总毛利额" end)"线上毛利额"
		--,sum(case when "线上/线下"='线上' then  "总客流" end) on_line_times
		--,sum(case when "线上/线下"='线下' then  "总毛利额" end)"线下总毛利额"
		--,sum(case when "线上/线下"='线下' then  "总客流" end) off_line_times
	from
	(
		select t.adms_org_name			--分公司名称
			,t.STRG_MGT_DEPT_NAME		--门管部名称
			,t.DIST_NAME				--片区名称
			,s.phmc_code
			,t.phmc_s_name				--门店名称
			,to_char(stsc_date,'yyyy-mm') year_month
			--,t.city
			,case when ORDR_SOUR_CODE in ('0100','0415',null,'') or ORDR_SOUR_CODE is null then 'off' else 'on' end as line_flag
			,sum(sale_amt) sale_total
			--,sum(GROS_PROF_AMT)"总毛利额"
			--,count(distinct case when ORDR_CATE_CODE<>'3' then sale_ordr_doc end )"总客流"
		from dw.fact_sale_ordr_detl s
		inner join dw.dim_phmc t on s.phmc_code=t.phmc_code
		left join dw.DIM_GOODS_H g on g.goods_sid=s.goods_sid
		left join 
		(
			 select dict_code,dict_name
			 from ds_pos.sys_dict
			 where TYPE_CODE='orderType'
				and DELETELABLE='0'
			 group by  dict_code,dict_name
		)t1 
		on s.ORDR_SOUR_CODE=t1.dict_code
		where stsc_date>='2019-05-01'
			and stsc_date< current_date
			and "PROD_CATE_LEV1_CODE"<>'Y86' 
			and "ORDR_CATE_CODE"<>'4'
		group by 
			adms_org_name
			,s.phmc_code
			,phmc_s_name
			,to_char(stsc_date,'yyyy-mm')
			,city
			,case when ORDR_SOUR_CODE in ('0100','0415',null,'') or ORDR_SOUR_CODE is null then 'off' else 'on' end
		)
	group by
		adms_org_name
		,city
		--,phmc_code
		--,phmc_s_name
		,year_month
)
,
--得到每个分公司每个城市近三个月线上销售占比增长率
t2 as(
	select adms_org_name
		,max(one_month_ago) as one_month_ago
		,max(two_month_ago) as two_month_ago
		,max(three_month_ago) as three_month_ago
	from
	(
		select adms_org_name
			,city
			,year_month
			,on_line_sale
			,case when year_month=to_char(add_months(current_date,-1),'yyyy-mm') then on_line_sale/off_line_sale else 0 end as one_month_ago
			,case when year_month=to_char(add_months(current_date,-2),'yyyy-mm') then on_line_sale/off_line_sale else 0 end as two_month_ago
			,case when year_month=to_char(add_months(current_date,-3),'yyyy-mm') then on_line_sale/off_line_sale else 0 end as three_month_ago
		from t1
	)
	group by adms_org_name
	
)
,
--得到城市四个状态
t3 as(
	select adms_org_name
		,case when one_month_ago>two_month_ago and two_month_ago>three_month_ago then 'increse'		--连续提升则为上升
			when one_month_ago<two_month_ago and two_month_ago<three_month_ago then 'decrese'		--连续下降则为下降
			when one_month_ago>three_month_ago then 'shake_up'		--最终上升则为震荡上升
			else 'shake_down'								--最终下降则为震荡下降
			end as grow_type
		,one_month_ago
		,two_month_ago
		,three_month_ago
		,one_month_ago-three_month_ago as increse_rate
	from t2
)

select *
from t3
order by grow_type,increse_rate desc












