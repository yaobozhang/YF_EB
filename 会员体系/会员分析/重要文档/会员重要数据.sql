--20191218
--by ybz
--���⣺�ճ��۲⵱ǰ��Ա����

--step1:���ȣ��õ���Ա��������
with t1 as (
	select t1.MEMB_CODE as member_id
		,t2.member_id as if_buy_lastyear
	from "DW"."FACT_MEMBER_BASE" t1
	left join 
	(
		select member_id
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
				 '20191218'),
				 'PLACEHOLDER' = ('$$BeginTime$$',
				 '20181218'))
		group by member_id
	) t2
	on t1.memb_code=t2.member_id
)
,

--�ܻ�Ա������Ծ��Ա��
t2 as (
	SELECT COUNT(MEMBER_ID) as memb_num
		,count(if_buy_lastyear) as memb_active_num
	FROM t1 

)
,
--�����ŵ���������������Ա��
--�����ŵ�����ҵ����
--�õ�������������
t3 as (
	select count(distinct customer_id) AS CHRONIC_NUM from "DS_ZT"."CHRONIC_PATIENT"
)
select t2.*,t3.* from t2 left join t3 on 1=1