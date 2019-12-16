--���ȣ��õ�ÿ���ֹ�˾��ÿ���Źܲ���ÿ��Ƭ����ÿ���ŵ�����������������

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
		--,sum(case when "����/����"='����' then  "��ë����" end)"����ë����"
		--,sum(case when "����/����"='����' then  "�ܿ���" end) on_line_times
		--,sum(case when "����/����"='����' then  "��ë����" end)"������ë����"
		--,sum(case when "����/����"='����' then  "�ܿ���" end) off_line_times
	from
	(
		select t.adms_org_name			--�ֹ�˾����
			,t.STRG_MGT_DEPT_NAME		--�Źܲ�����
			,t.DIST_NAME				--Ƭ������
			,s.phmc_code
			,t.phmc_s_name				--�ŵ�����
			,to_char(stsc_date,'yyyy-mm') year_month
			--,t.city
			,case when ORDR_SOUR_CODE in ('0100','0415',null,'') or ORDR_SOUR_CODE is null then 'off' else 'on' end as line_flag
			,sum(sale_amt) sale_total
			--,sum(GROS_PROF_AMT)"��ë����"
			--,count(distinct case when ORDR_CATE_CODE<>'3' then sale_ordr_doc end )"�ܿ���"
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
--�õ�ÿ���ֹ�˾ÿ�����н���������������ռ��������
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
--�õ������ĸ�״̬
t3 as(
	select adms_org_name
		,case when one_month_ago>two_month_ago and two_month_ago>three_month_ago then 'increse'		--����������Ϊ����
			when one_month_ago<two_month_ago and two_month_ago<three_month_ago then 'decrese'		--�����½���Ϊ�½�
			when one_month_ago>three_month_ago then 'shake_up'		--����������Ϊ������
			else 'shake_down'								--�����½���Ϊ���½�
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












