1.根据会员第一笔消费时间，取出所有历史第一笔消费在2018（2019）年以后的会员 
得到表A（会员编码，消费日期，商品编码，商品1级品类，商品2级品类，商品自定义品类，是否刚需，商品数量，商品金额）

2.根据表A计算所有会员累积消费金额以及本次消费所有品类以及本次消费商品件数以及是否刚需 
得到表B（会员编码，消费日期，第几次消费，消费金额，消费品类，消费件数，是否刚需）

3.根据表B 对会员打标，三种情况分别打标累积品类数、消费商品件数、刚需非刚需 
得到表C（会员编码，消费日期，消费次数，累积金额，消费品类，消费件数，是否刚需）

4.分别算出，每一个会员的三个分布基础数据

表X【会员单品类到多品类累积消费金额表】(会员编码，1品类日期，1品类累积消费金额，2品类日期，2品类消费金额，3品类日期，3品类消费金额)

表Y【会员单件到多件累积消费金额表】（会员编码，1件日期，1件累积消费金额，2件日期，2件累积消费金额，3件日期，3件累积消费金额）

表Z【会员刚需到非刚需累积消费金额表】（会员编码，刚需开始日期，刚需开始消费金额，非刚需开始日期，非刚需累积消费金额）

5.分别对表X/Y/Z 做分布，取会员数最多的区间段作为最终结果

-- 建A表
create column table "EXT_TMP"."YBZ_MEMBER_ANY" as
(
        select s.stsc_date,                                        	--日期
                s.member_id,                                        --会员ID
                s.GOODS_CODE,
                s.GOODS_NAME,
                s.PROD_CATE_LEV1_CODE,                              --品类分析专用，不用时注释
                s.PROD_CATE_LEV1_NAME,
                s.PROD_CATE_LEV2_CODE,
                s.PROD_CATE_LEV2_NAME,
                CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
                  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
                  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV2_NAME								-- 二级品类（拼接了是否处方药）
                ,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
                  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
                  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV1_NAME   							--  用于判断是否刚需
                ,SALE_AMT 	-- 销售额
                ,SALE_QTY  	-- 销售数量
        from (
                SELECT
                         t."STSC_DATE",                                            --销售日期
                         t."GOODS_CODE",                                           --商品编码
                         t."MEMBER_ID",                                            --会员编码
                         g.PROD_CATE_LEV1_CODE,                                    --品类分析专用，不用时注释
                         g.PROD_CATE_LEV1_NAME,
                         g.PROD_CATE_LEV2_CODE,										-- 品类二级
                         g.PROD_CATE_LEV2_NAME,
                         g.GOODS_NAME,
                         sum(t."GOODS_SID") AS "GOODS_SID",                        --商品编码关联商品表唯一编码
                         sum("SALE_QTY") AS "SALE_QTY",                            --销售数量
                         sum("SALE_AMT") AS "SALE_AMT"                             --销售额
                FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
                         '20190101'),
                         'PLACEHOLDER' = ('$$BeginTime$$',
                         '20180101')) t 
                left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
                where t.member_id is not null
                AND NOT EXISTS(
                        SELECT 1 FROM DW.FACT_MEMBER_BASE t1
                        WHERE t.member_id=t1.memb_code
                        and t1.FIRST_CONSUME_TIME>='20180101'
                )
                GROUP BY t."STSC_DATE",                                  --销售日期
                         t."GOODS_CODE",                                 --商品编码
                         t."MEMBER_ID",                                                                         --会员编码
                         g.PROD_CATE_LEV1_CODE,                                                --品类分析专用，不用时注释
                         g.PROD_CATE_LEV1_NAME,
                         g.PROD_CATE_LEV2_CODE,
                         g.PROD_CATE_LEV2_NAME,
                         g.GOODS_NAME
                ) s
        left join (select GOODS_CODE,'1' AS PRES_FLAG from "DW"."DIM_GOODS_CONG" where PRES_TYPE_CODE IN ('1','2','3')) t
        on S.GOODS_CODE = T.GOODS_CODE
)

/* 根据表A计算所有会员累积消费金额以及本次消费所有品类以及本次消费商品件数以及是否刚需 
得到表B（会员编码，消费日期，第几次消费，消费金额，消费品类，消费件数，是否刚需） */

一个会员同一天算一次

1、需要计算累计消费金额
2、需要计算累计品类个数


/*
select member_id  -- 会员编码
	  ,stsc_date
	 ,goods_code
	 ,goods_name 
	 ,SELF_CATE_LEV2_NAME
	 ,sale_amt
	 ,sale_qty
from "EXT_TMP"."YBZ_MEMBER_ANY" WHERE MEMBER_ID='5EA1A6A5E695623FE053330302013F4C'
ORDER BY member_id,stsc_date
;
*/

/*
-- 逻辑
with T1 AS (
	SELECT *
	,SUM(SALE_AMT)OVER(PARTITION BY member_id ORDER BY STSC_DATE) AS ACCU_AMT  	-- 每一次累计金额
	FROM 
	(
	select member_id  -- 会员编码
		  ,stsc_date
		 ,SELF_CATE_LEV2_NAME  				-- 品类二级
		 ,count(1)  as SELF_CATE_NUM 		-- 品类二级消费个数
		 ,sum(sale_amt)  as sale_amt		-- 销售金额
	from "EXT_TMP"."YBZ_MEMBER_ANY" WHERE MEMBER_ID='0ee20a10b0834524b5d635ef678d265d'
	group BY member_id  -- 会员编码
		  ,stsc_date
		 ,SELF_CATE_LEV2_NAME
	ORDER BY member_id,stsc_date
	)
),t2 as (--品类的最早时间
	SELECT  member_id
		,SELF_CATE_LEV2_NAME
		,MIN(t1.stsc_date) AS MIN_DATE
	FROM T1 
	GROUP BY member_id
		,SELF_CATE_LEV2_NAME
	ORDER BY MIN(stsc_date)
),t3 as (
select *,row_number()over(partition by member_id order by MIN_DATE) as cnt from t2 
)
select distinct T1.member_id
	,floor(T1.ACCU_AMT) AS ACCU_AMT
from t1 
inner join t3 on t3.cnt=3 AND T1.MEMBER_ID=T3.MEMBER_ID AND T1.STSC_DATE=T3.MIN_DATE
*/

-- 所有会员品类表
drop table ext_tmp.zj_member_any_0 ;
create column table ext_tmp.zj_member_any_0 as (
	SELECT *
	,SUM(SALE_AMT)OVER(PARTITION BY member_id ORDER BY STSC_DATE) AS ACCU_AMT  	-- 每一次累计金额
	FROM 
	(
	select member_id  -- 会员编码
		  ,stsc_date
		 ,SELF_CATE_LEV2_NAME  				-- 品类二级
		 ,count(1)  as SELF_CATE_NUM 		-- 品类二级消费个数
		 ,sum(sale_amt)  as sale_amt		-- 销售金额	
	from "EXT_TMP"."YBZ_MEMBER_ANY" -- WHERE MEMBER_ID='0ee20a10b0834524b5d635ef678d265d'
	group BY member_id  -- 会员编码
		  ,stsc_date
		 ,SELF_CATE_LEV2_NAME
	ORDER BY member_id,stsc_date
	)
)
;

-- 所有会员消费买到第三个品类对应的累计金额 
drop table ext_tmp.zj_member_any_1;
create column table  ext_tmp.zj_member_any_1 as (
select distinct T1.member_id
	,floor(T1.ACCU_AMT) AS ACCU_AMT
from ext_tmp.zj_member_any_0 t1
inner join 
(
select *,row_number()over(partition by member_id order by MIN_DATE) as cnt
from  
(
SELECT member_id
		,SELF_CATE_LEV2_NAME
		,MIN(t1.stsc_date) AS MIN_DATE
	FROM ext_tmp.zj_member_any_0 T1 
	GROUP BY member_id
		,SELF_CATE_LEV2_NAME
	ORDER BY MIN(stsc_date)
	)t2
)t3 on t3.cnt=3 AND T1.MEMBER_ID=T3.MEMBER_ID AND T1.STSC_DATE=T3.MIN_DATE
)


select * from ext_tmp.zj_member_any_1 where member_id='0ee20a10b0834524b5d635ef678d265d'

 

select 
ACCU_AMT,
count(1)
from 
(
        SELECT 
        b.MEMBER_ID
        ,FLOOR(b.ACCU_AMT/10)*10  ACCU_AMT
        FROM ext_tmp.zj_member_any_2 b
)
group by ACCU_AMT
order by ACCU_AMT




















