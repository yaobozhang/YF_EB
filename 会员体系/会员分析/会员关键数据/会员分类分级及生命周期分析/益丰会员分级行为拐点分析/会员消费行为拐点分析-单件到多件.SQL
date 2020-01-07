
-- 会员从第一次消费开始到多少钱会员发生以下行为：从单件数到多件数(从买1件到买2件到买3件)
-- 1、找出第一次有效消费在2018，2019年消费的会员 
WITH FST_MEMBER_TMP AS( 
SELECT 
  F.MEMBER_ID ,   -- 会员
  MIN(F.STSC_DATE) AS FST_STSC_DATE , -- 第一次消费时间
FROM DW.FACT_SALE_ORDR_DETL F 
WHERE F.STSC_DATE >= '20140101'
  AND F.STSC_DATE < '20200101'
  AND F.MEMBER_ID = '0ee20a10b0834524b5d635ef678d265d' 
  AND F.MEMBER_ID IS NOT NULL 
  AND F.CV_FLAG = 'Y' 
  AND F.GOODS_CODE NOT IN ('8000874','8000875')
  AND F.PROD_CATE_LEV1_CODE NOT IN ('Y85','Y86') 
GROUP BY F.MEMBER_ID 
HAVING YEAR(MIN(F.STSC_DATE)) IN ('2018','2019') 
) 
SELECT 
F.MEMBER_ID ,
F.GOODS_CODE ,
F.SALE_QTY ,    -- 销售数量
F.SALE_AMT ,    -- 销售金额
F.STSC_DATE     -- 消费日期
FROM DW.FACT_SALE_ORDR_DETL F 
where exists (FST_MEMBER_TMP F1 ON F.MEMBER_ID = F1.MEMBER_ID 


--------------------------- bozhang 
create column table "EXT_TMP"."YBZ_MEMBER_ANY" as
(
        select s.stsc_date,                                        --日期
                s.member_id,                                        --会员ID
                s.GOODS_CODE,
                s.GOODS_NAME,
                s.PROD_CATE_LEV1_CODE,                                                --品类分析专用，不用时注释
                s.PROD_CATE_LEV1_NAME,
                s.PROD_CATE_LEV2_CODE,
                s.PROD_CATE_LEV2_NAME,
                CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'处方药'
                  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then PROD_CATE_LEV2_NAME||'非处方药'
                  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV2_NAME
                ,CASE WHEN IFNULL(T.PRES_FLAG,2) =  1 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '处方药'
                  WHEN IFNULL(T.PRES_FLAG,2) =  2 AND s.PROD_CATE_LEV1_CODE = 'Y01' then '非处方药'
                  ELSE IFNULL(s.PROD_CATE_LEV1_NAME,'其他') END SELF_CATE_LEV1_NAME
                ,SALE_AMT
                ,SALE_QTY
        from (
                SELECT
                         t."STSC_DATE",                                                                  --销售日期
                         t."GOODS_CODE",                                                            --商品编码
                         t."MEMBER_ID",                                                                --会员编码
                         g.PROD_CATE_LEV1_CODE,                                                --品类分析专用，不用时注释
                         g.PROD_CATE_LEV1_NAME,
                         g.PROD_CATE_LEV2_CODE,
                         g.PROD_CATE_LEV2_NAME,
                         g.GOODS_NAME,
                         sum(t."GOODS_SID") AS "GOODS_SID",                        --商品编码关联商品表唯一编码
                         sum("SALE_QTY") AS "SALE_QTY",                                --销售数量
                         sum("SALE_AMT") AS "SALE_AMT"                                --销售额
                FROM "_SYS_BIC"."YF_BI.DW.CRM/CV_REEF_SALE_ORDER_DETL"('PLACEHOLDER' = ('$$EndTime$$',
                         '20190101'),
                         'PLACEHOLDER' = ('$$BeginTime$$',
                         '20180101')) t 
                left join dw.DIM_GOODS_H g on g.goods_sid=t.goods_sid
                where t.member_id is not null
                AND EXISTS(
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
);

-- 偲偲 
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

-- 会员从第一次消费开始到多少钱会员发生以下行为：从单件数到多件数(从买1件到买2件到买3件)
-- 1、找出第一次有效消费在2018，2019年消费的会员 : EXT_TMP.YBZ_MEMBER_ANY   286,219,529 

-- 2、单件数到多件数先从每个会员每天的消费数量来看，所以取出每个会员每天的消费数量数据来做判断   141,871,095 
CREATE COLUMN TABLE EXT_TMP.MEMBER_ORDR_TMP AS(
SELECT 
MEMBER_ID ,                     -- 会员编号
STSC_DATE ,                     -- 消费日期
MAX(SALE_QTY) AS SALE_QTY  ,    -- 最大消费数量
SUM(SALE_AMT) AS SALE_AMT       -- 会员每天的消费金额
FROM EXT_TMP.YBZ_MEMBER_ANY 
GROUP BY MEMBER_ID , STSC_DATE
) ;  


-- 获取目标会员数据的中间临时表  141,871,095
create table ext_tmp.target_member_mid AS (
  SELECT 
			MEMBER_ID ,
			SALE_QTY ,
			SALE_AMT ,
			STSC_DATE ,
			SUM(SALE_AMT) OVER(PARTITION BY MEMBER_ID ORDER BY STSC_DATE) AS SUM_SALE_AMT , 
			ROW_NUMBER() OVER(PARTITION BY MEMBER_ID ORDER BY STSC_DATE) as rn  
			FROM MEMBER_ORDR_TMP 
			-- WHERE MEMBER_ID = 'A0F41A4E-2831-4628-89BC-ACC7BE6B1357'  -- '0ee20a10b0834524b5d635ef678d265d' --  
			GROUP BY 
			MEMBER_ID ,
			SALE_QTY ,
			SALE_AMT ,
			STSC_DATE 
); 


-- 获取目标会员数据的结果临时表，排除第一笔消费的数量不是1的会员同时多个相同的件数取时间最早的一个   22,945,395 
create table ext_tmp.member_tmp_lxx as (
select
MEMBER_ID ,
SALE_QTY ,
SALE_AMT ,
STSC_DATE ,
SUM_SALE_AMT
from (
			select
			MEMBER_ID ,
			SALE_QTY ,
			SALE_AMT ,
			STSC_DATE ,
			SUM_SALE_AMT
			, ROW_NUMBER() OVER(PARTITION BY MEMBER_ID,SALE_QTY ORDER BY STSC_DATE) AS RN -- 多个件数相同的会员，取时间最早的一个
			from (        SELECT 
							MEMBER_ID ,
							SALE_QTY ,
							SALE_AMT ,
							STSC_DATE ,
							SUM_SALE_AMT
							FROM ext_tmp.target_member_mid t
							where exists (select 1 from ext_tmp.target_member_mid t1 where t1.rn = 1 and t1.sale_qty = 1 and t.MEMBER_ID = t1.MEMBER_ID )
							-- and   MEMBER_ID = '0ee20a10b0834524b5d635ef678d265d'
					 ) a 
		 )
WHERE RN = 1 
) ; 


-- 找到件数1到2的点
select 
SUM_SALE_AMT,
count(1)
from 
(
  SELECT 
  b.MEMBER_ID
  ,FLOOR(b.SUM_SALE_AMT/10)*10 AS SUM_SALE_AMT
  FROM ext_tmp.member_tmp_lxx b
  where SALE_QTY = 2 
)
group by SUM_SALE_AMT
order by SUM_SALE_AMT;






