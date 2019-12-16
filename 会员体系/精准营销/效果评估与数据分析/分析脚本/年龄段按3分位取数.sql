-- 所有集团会员，按百分位看分布

-- 根据年龄做分布分析



with t1 as 
(select MEMB_CARD_CODE,age,BIRT_DATE --,row_number() over(partition by age order by age) as rn
	from 
		(select MEMB_CARD_CODE,BIRT_DATE,floor(months_between(to_date(BIRT_DATE),CURRENT_DATE )/12) as age
			from 
				DW.FACT_MEMBER_BASE
			where BIRT_DATE<>'' and BIRT_DATE is not null and BIRT_DATE>='1918-1-3' and BIRT_DATE<='2008-1-3'
		))
select distinct PERCENTILE_DISC (0.10) WITHIN GROUP ( ORDER BY age ASC) over()  percent_10,
	 PERCENTILE_DISC (0.20) WITHIN GROUP ( ORDER BY age ASC) over()  percent_20,
	 PERCENTILE_DISC (0.30) WITHIN GROUP ( ORDER BY age ASC) over()  percent_30,
	 PERCENTILE_DISC (0.40) WITHIN GROUP ( ORDER BY age ASC) over()  percent_40,
	 PERCENTILE_DISC (0.50) WITHIN GROUP ( ORDER BY age ASC) over()  percent_50,
	 PERCENTILE_DISC (0.60) WITHIN GROUP ( ORDER BY age ASC) over()  percent_60,	
	 PERCENTILE_DISC (0.70) WITHIN GROUP ( ORDER BY age ASC) over()  percent_70,
	 PERCENTILE_DISC (0.80) WITHIN GROUP ( ORDER BY age ASC) over()  percent_80,	
	 PERCENTILE_DISC (0.90) WITHIN GROUP ( ORDER BY age ASC) over()  percent_90,
	 PERCENTILE_DISC (0.99) WITHIN GROUP ( ORDER BY age ASC) over()  percent_99
from t1;



PERCENT_10;PERCENT_20;PERCENT_30;PERCENT_40;PERCENT_50;PERCENT_60;PERCENT_70;PERCENT_80;PERCENT_90;PERCENT_100
25;30;33;37;41;46;51;58;66;110



select max(age),min(age) from 
(
select MEMB_CARD_CODE,BIRT_DATE,floor(months_between(to_date(BIRT_DATE),CURRENT_DATE )/12) as age
			from 
				DW.FACT_MEMBER_BASE
			where BIRT_DATE<>'' and BIRT_DATE is not null and BIRT_DATE>'1908-1-3' and BIRT_DATE<CURRENT_DATE)