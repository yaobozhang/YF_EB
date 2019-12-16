--目的：根据会员商品偏好数据，得到每个会员最偏好商品，统计最偏好商品会员数排名前100商品，同样逻辑依次得到排名前五TOP100商品
		
--取每个人的最佳偏好商品
with t1 as(
	select  member_id 
		,good_code 
		,score
		,Row_Number() OVER(partition by member_id ORDER BY score desc) rank
	from  DM.userb_user_goods_prefer
)


----取最佳商品前100（按商品次数排名 降序取前100）
,t2 as (
	select  good_code,rank,count(*) as nums from t1  where rank<=5
	group by good_code ,rank 
)
,t3 as (
	select   
		good_code 
		,rank
		,Row_Number() OVER(partition by rank ORDER BY nums desc) rn
	from t2
)
,t4 as
(
	select * from t3 where rn<=200 
)

select distinct good_code 
				,goods_name 
from t4  a
left join (select  * from  DW.DIM_GOODS_H  where END_DATE='9999-12-31') b 
on a.good_code=b.goods_code

---------------
