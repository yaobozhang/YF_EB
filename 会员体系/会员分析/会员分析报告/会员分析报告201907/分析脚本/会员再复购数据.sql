alter PROCEDURE ext_tmp.YBZ_MEMB_REBUY  ()
LANGUAGE SQLSCRIPT SQL SECURITY INVOKER
AS
--call ext_tmp.YBZ_MEMB_REBUY()
BEGIN
--得到会员每年数据并建表 
VAR1=
	SELECT AT_YEAR
		,MEMBER_ID
		,SALE_AMT
		--,row_number() OVER (PARTITION BY member_id ORDER BY AT_YEAR asc) rk
	FROM
	(
		SELECT
			 to_char("STSC_DATE",'YYYY') as at_year,  								--销售日期
			 t1."MEMBER_ID",								--会员编码
			SUM("SALE_AMT") AS SALE_AMT					--消费金额
		FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
			 '20191231'),			--数据量过大，两年两年插入
			 'PLACEHOLDER' = ('$$BeginTime$$',
			 '20180101')) t1
		GROUP BY to_char("STSC_DATE",'YYYY'),                                  --销售日期
			 t1."MEMBER_ID" 
	)t1
	where exists(
		select 1 from dw.fact_member_base t2		--找到已有会员
		where t1.MEMBER_ID=t2.MEMB_CODE
	)

;
/*
create column table "EXT_TMP"."YBZ_MEMBER_1" as
(
	SELECT AT_YEAR
		,MEMBER_ID
		,SALE_AMT
	FROM :VAR1
)
;
*/
insert into "EXT_TMP"."YBZ_MEMBER_1"
(
	AT_YEAR
	,MEMBER_ID
	,SALE_AMT
)
SELECT AT_YEAR
	,MEMBER_ID
	,SALE_AMT
FROM :VAR1
COMMIT;



END;