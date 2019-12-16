--取一月份订单
with  t1 as (
	select  
		SALE_ORDR_DOC   	--订单编号
		,GOODS_CODE				--商品编号
		,SALE_AMT			--销售额
		,GROS_PROF_AMT	--毛利额
from DW.FACT_SALE_ORDR_DETL
where ORDR_SALE_TIME >='20190101' and  ORDR_SALE_TIME <='20190131'
and SALE_AMT >0
)

--select  count(distinct GOODS_CODE) from t1  --34,438

--select  count( distinct SALE_ORDR_DOC) from t1  --21,261,257  /10,839,191


---关联红标商品表  取红标商品
,t2 as (
select  
	distinct GOOD_CODE   
	,GOODS_NAME
from  DS_POS.SALES_REDLABEL_GOODS
)

--select  count(distinct GOOD_CODE) from  t2   --38,382

---取订单表中 红标商品销售 毛利    计算每笔订单的毛利率

,t3  as (
select  SALE_ORDR_DOC   	--订单编号
		--,case when sum(SALE_AMT) !=0 and sum(SALE_AMT) is not null then ceil(sum (GROS_PROF_AMT)/(sum(SALE_AMT))*10) else -1000 end as gros_per
		,case when sum(SALE_AMT) >0 and sum(SALE_AMT) is not null then ceil((sum (GROS_PROF_AMT)/sum(SALE_AMT))*10) end as gros_per
from t1
inner join t2
on t1.GOODS_CODE=t2.GOOD_CODE
group by SALE_ORDR_DOC
)


--select * from   t3 where gros_per=2860


--select  max(gros_per),min(gros_per) from  t3

select distinct 
				PERCENTILE_DISC (0) WITHIN GROUP ( ORDER BY gros_per ASC) over() as value_0   
				,PERCENTILE_DISC (0.1) WITHIN GROUP ( ORDER BY gros_per ASC) over() as  value_01
				,PERCENTILE_DISC (0.2) WITHIN GROUP ( ORDER BY gros_per ASC) over()  as value_02
				,PERCENTILE_DISC (0.3) WITHIN GROUP ( ORDER BY gros_per ASC) over() as value_03
				,PERCENTILE_DISC (0.4) WITHIN GROUP ( ORDER BY gros_per ASC) over() as  value_04
				,PERCENTILE_DISC (0.5) WITHIN GROUP ( ORDER BY gros_per ASC) over() as  value_05
				,PERCENTILE_DISC (0.6) WITHIN GROUP ( ORDER BY gros_per ASC) over()  as value_06
				,PERCENTILE_DISC (0.7) WITHIN GROUP ( ORDER BY gros_per ASC) over() as value_07
				,PERCENTILE_DISC (0.8) WITHIN GROUP ( ORDER BY gros_per ASC) over() as value_08
				,PERCENTILE_DISC (0.9) WITHIN GROUP ( ORDER BY gros_per ASC) over() as value_09
				,PERCENTILE_DISC (1.0) WITHIN GROUP ( ORDER BY gros_per ASC) over()  as value_10		
from t3



--ceil((sum (GROS_PROF_AMT)/sum(SALE_AMT))*10) 
--else -1000 
---------------------------------------------------------取数创建表并插入数据
create column table ext_tmp.BOZHANG_20190301
as
(
select SALE_ORDR_DOC,ceil(GROS_PROF_AMT/SALE_AMT*10) as gros_per
from
(
 select  SALE_ORDR_DOC    --订单编号
   ,sum(SALE_AMT) as SALE_AMT
   ,sum(GROS_PROF_AMT) as GROS_PROF_AMT
 from (
  select  
    SALE_ORDR_DOC    --订单编号
    ,GOODS_CODE    --商品编号
    ,SALE_AMT   --销售额
    ,GROS_PROF_AMT --毛利额
  from DW.FACT_SALE_ORDR_DETL
  where ORDR_SALE_TIME >='20190101' and  ORDR_SALE_TIME <='20190131'
  and SALE_AMT >0
 )t1
 where exists(
  select 1 from DS_POS.SALES_REDLABEL_GOODS t2
  where t1.GOODS_CODE=t2.GOOD_CODE
 )
 group by SALE_ORDR_DOC
)
)
