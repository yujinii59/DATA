WITH MAIN AS
(
SELECT T1.DP_VRSN_ID
			  , T1.SALES_MGMT_CD	
			  , T1.ITEM_CD
			  , T1.PLAN_YYMMDD
			  , T1.USER_CD
			  , T1.PLAN_WEEK
			  , T1.PLAN_PART_WEEK
			  , T1.PLAN_YYMM
			  , CAST(SUM(ISNULL(T1.DP_QTY,0)) AS INT) AS SP2_SELL_IN_QTY
			  , CAST(SUM(ISNULL(T3.DP_QTY,0)) AS INT) AS SP1_SELL_IN_QTY
              , CAST(SUM(ISNULL(CASE WHEN 'AGN_PRICE' = 'CON_PRICE' THEN T4.CON_PRICE
                                     WHEN 'AGN_PRICE' = 'FAC_PRICE' THEN T4.FAC_PRICE
                                     WHEN 'AGN_PRICE' = 'SHP_PRICE' THEN T4.SHP_PRICE
                                     WHEN 'AGN_PRICE' = 'AGN_PRICE' THEN T4.AGN_PRICE
                                     ELSE 0 END,0)) AS INT) AS PRICE
              , CAST(SUM(ISNULL(T5.RST_QTY,0)) AS INT) AS RST_QTY
              , 0 AS DF_SELL_IN_QTY
              , 0 AS DF_SELL_OUT_QTY
              , 0 AS FUNC_COL_01  -- 회전계획
              , 0 AS FUNC_COL_02  -- WOS Target
              , 0 AS FUNC_COL_03  -- WOS 예상
              , 0 AS FUNC_COL_04  -- 예상재고
              , 0 AS FUNC_COL_05  -- 매출액
		  FROM M4S_O201002 T1	      
			   INNER JOIN M4S_O201001 T3
			   ON  T1.ITEM_CD = T3.ITEM_CD
			   AND T1.PLAN_YYMMDD = T3.PLAN_YYMMDD
			   AND T1.SALES_MGMT_CD = T3.SALES_MGMT_CD
			   AND T1.USER_CD = T3.USER_CD
			   AND T1.DP_VRSN_ID = T3.DP_VRSN_ID
               LEFT JOIN M4S_I002041 T4
               ON  T1.PROJECT_CD = T4.PROJECT_CD
               AND T1.ITEM_CD = T4.ITEM_CD
               AND T1.PLAN_YYMMDD >= T4.PRICE_START_YYMMDD
               AND T4.PRICE_QTY_UNIT_CD = 'BOX'
               LEFT HASH JOIN (SELECT SELLIN.ITEM_CD
                                ,SH.SALES_MGMT_CD
			                    ,CAL.WEEK
			                    ,CAL.PART_WEEK
                                ,CAL.START_PART_WEEK_DAY
			                    ,SUM(RST_SALES_QTY) AS RST_QTY
		                    FROM (SELECT ITEM_CD
                                        ,SOLD_CUST_GRP_CD
                                        ,SHIP_CUST_GRP_CD
                                        ,CONVERT(VARCHAR(8),DATEADD(YEAR,1,YYMMDD),112) AS YYMMDD
                                        ,RST_SALES_QTY
                                    FROM M4S_I002170 
                                   WHERE YYMM BETWEEN '202010' AND '202102' ) SELLIN 
                                ,M4S_I002060 CST
                                ,M4S_I204050 SH
                                ,M4S_I204010 SV
                                ,M4S_O201010 SPV
                                ,M4S_I002030 CAL
		                   WHERE 1=1
                             AND SELLIN.SOLD_CUST_GRP_CD = CST.CUST_CD
                             AND SELLIN.ITEM_CD = SH.ITEM_CD
                             AND CST.CUST_GRP_CD = SUBSTRING(SH.SALES_MGMT_CD,8,4)
                             AND SH.SALES_MGMT_VRSN_ID = SV.SALES_MGMT_VRSN_ID
                             AND SV.SALES_MGMT_VRSN_ID = SPV.SALES_MGMT_VRSN_ID
                             AND SPV.DP_VRSN_ID = 'SP_2021W41.01'
                             AND SELLIN.YYMMDD = CAL.YYMMDD
		                   GROUP BY SELLIN.ITEM_CD
                                   ,SH.SALES_MGMT_CD
		  	                       ,CAL.WEEK
			                       ,CAL.PART_WEEK
                                   ,CAL.START_PART_WEEK_DAY ) T5
                ON  T1.ITEM_CD = T5.ITEM_CD
                AND T1.SALES_MGMT_CD = T5.SALES_MGMT_CD
                AND T1.PLAN_YYMMDD = T5.START_PART_WEEK_DAY
          WHERE 1=1
		    AND T1.PROJECT_CD = 'ENT001' 
		    AND T1.DP_VRSN_ID = 'SP_2021W41.01' 
		  GROUP BY T1.DP_VRSN_ID
			      ,T1.SALES_MGMT_CD	
			      ,T1.ITEM_CD
			      ,T1.PLAN_YYMMDD
			      ,T1.USER_CD
			      ,T1.PLAN_WEEK
			      ,T1.PLAN_PART_WEEK
			      ,T1.PLAN_YYMM 
)
,UNPVT AS
(
SELECT DP_VRSN_ID
      ,SALES_MGMT_CD
      ,ITEM_CD
      ,USER_CD
      ,PLAN_YYMMDD
      ,PLAN_YYMM
      ,PLAN_WEEK
      ,PLAN_PART_WEEK
      ,VALUE_NAME
      ,MAIN_VALUE
  FROM MAIN
  UNPIVOT(MAIN_VALUE FOR VALUE_NAME IN ([SP2_SELL_IN_QTY]
                                       ,[SP1_SELL_IN_QTY]
                                       ,[PRICE]
                                       ,[RST_QTY]
                                       ,[DF_SELL_IN_QTY]
                                       ,[DF_SELL_OUT_QTY]
                                       ,[FUNC_COL_01]
                                       ,[FUNC_COL_02]
                                       ,[FUNC_COL_03]
                                       ,[FUNC_COL_04]
                                       ,[FUNC_COL_05])) AS UNPVT
)
SELECT '' AS RESERVE_01
      ,'' AS RESERVE_02
      ,'' AS RESERVE_03
      ,'' AS RESERVE_04
      ,'' AS RESERVE_05
      ,'' AS RESERVE_06
      ,'' AS RESERVE_07
      ,'' AS RESERVE_08
      ,'' AS RESERVE_09
      ,'' AS RESERVE_10
      ,'' AS RESERVE_11
      ,'' AS RESERVE_12
      ,'' AS RESERVE_13
      ,'' AS RESERVE_14
      ,'' AS RESERVE_15
      ,'' AS RESERVE_16
      ,'' AS RESERVE_17
      ,'' AS RESERVE_18
      ,'' AS RESERVE_19
      ,'' AS RESERVE_20
      ,T1.DP_VRSN_ID
      ,T1.SALES_MGMT_CD
      ,T1.ITEM_CD
      ,T1.USER_CD
      ,T1.VALUE_NAME
      ,CASE WHEN T1.VALUE_NAME = 'DF_SELL_OUT_QTY' THEN 1      -- 수요예측(SELL_OUT)
            WHEN T1.VALUE_NAME = 'FUNC_COL_01'     THEN 2      -- 회전계획
            WHEN T1.VALUE_NAME = 'FUNC_COL_02'     THEN 3      -- WOS TARGET
            WHEN T1.VALUE_NAME = 'FUNC_COL_04'     THEN 4      -- 예상재고
            WHEN T1.VALUE_NAME = 'RST_QTY'         THEN 5      -- 전년실적
            WHEN T1.VALUE_NAME = 'DF_SELL_IN_QTY'  THEN 6      -- 수요예측(SELL_IN)
            WHEN T1.VALUE_NAME = 'FUNC_COL_03'     THEN 7      -- WOS 예상
            WHEN T1.VALUE_NAME = 'SP1_SELL_IN_QTY' THEN 8      -- SP1 판매계획(SELL_IN)
            WHEN T1.VALUE_NAME = 'SP2_SELL_IN_QTY' THEN 9      -- SP2 판매계획(SELL_IN)
            WHEN T1.VALUE_NAME = 'PRICE'           THEN 10     -- 판매가
            WHEN T1.VALUE_NAME = 'FUNC_COL_05'     THEN 11     -- 매출액
            END AS ORDER_NUM
	  ,T3.ITEM_ATTR01_CD
	  ,T3.ITEM_ATTR02_CD
	  ,T3.ITEM_ATTR03_CD
	  ,T3.ITEM_ATTR04_CD
	  ,T3.ITEM_ATTR05_CD
      -- 여기까지는 KEY
      ,T4.SALES_MGMT_NM
	  ,T3.ITEM_ATTR01_NM AS ITEM_HIERARCHY_01
	  ,T3.ITEM_ATTR02_CD
	  ,T3.ITEM_ATTR02_NM AS ITEM_HIERARCHY_02
	  ,T3.ITEM_ATTR03_CD
	  ,T3.ITEM_ATTR03_NM AS ITEM_HIERARCHY_03
	  ,T3.ITEM_ATTR04_CD
	  ,T3.ITEM_ATTR04_NM AS ITEM_HIERARCHY_04
      ,T3.ITEM_NM
      ,CASE WHEN T1.VALUE_NAME = 'DF_SELL_OUT_QTY' THEN '수요예측(SELL_OUT)'
            WHEN T1.VALUE_NAME = 'FUNC_COL_01'     THEN '회전계획'
            WHEN T1.VALUE_NAME = 'FUNC_COL_02'     THEN 'WOS TARGET'
            WHEN T1.VALUE_NAME = 'FUNC_COL_04'     THEN '예상재고'
            WHEN T1.VALUE_NAME = 'RST_QTY'         THEN '전년실적'
            WHEN T1.VALUE_NAME = 'DF_SELL_IN_QTY'  THEN '수요예측(SELL_IN)'
            WHEN T1.VALUE_NAME = 'FUNC_COL_03'     THEN 'WOS 예상'
            WHEN T1.VALUE_NAME = 'SP1_SELL_IN_QTY' THEN 'SP1 판매계획(SELL_IN)'
            WHEN T1.VALUE_NAME = 'SP2_SELL_IN_QTY' THEN 'SP2 판매계획(SELL_IN)'
            WHEN T1.VALUE_NAME = 'PRICE'           THEN '판매가'
            WHEN T1.VALUE_NAME = 'FUNC_COL_05'     THEN '매출액'
            END AS ORDER_NAME
      ,SUM(CASE WHEN T2.TIME_INDEX = 1 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_01
      ,SUM(CASE WHEN T2.TIME_INDEX = 2 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_02
      ,SUM(CASE WHEN T2.TIME_INDEX = 3 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_03
      ,SUM(CASE WHEN T2.TIME_INDEX = 4 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_04
      ,SUM(CASE WHEN T2.TIME_INDEX = 5 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_05
      ,SUM(CASE WHEN T2.TIME_INDEX = 6 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_06
      ,SUM(CASE WHEN T2.TIME_INDEX = 7 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_07
      ,SUM(CASE WHEN T2.TIME_INDEX = 8 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_08
      ,SUM(CASE WHEN T2.TIME_INDEX = 9 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_09
      ,SUM(CASE WHEN T2.TIME_INDEX = 10 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_10
      ,SUM(CASE WHEN T2.TIME_INDEX = 11 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_11
      ,SUM(CASE WHEN T2.TIME_INDEX = 12 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_12
      ,SUM(CASE WHEN T2.TIME_INDEX = 13 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_13
      ,SUM(CASE WHEN T2.TIME_INDEX = 14 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_14
      ,SUM(CASE WHEN T2.TIME_INDEX = 15 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_15
      ,SUM(CASE WHEN T2.TIME_INDEX = 16 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_16
      ,SUM(CASE WHEN T2.TIME_INDEX = 17 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_17
      ,SUM(CASE WHEN T2.TIME_INDEX = 18 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_18
      ,SUM(CASE WHEN T2.TIME_INDEX = 19 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_19
      ,SUM(CASE WHEN T2.TIME_INDEX = 20 THEN T1.MAIN_VALUE ELSE 0 END) AS VALUE_WEEK_20  -- 20주차까지 (PART WEEK 기준)
  FROM UNPVT T1
      ,(SELECT T2.START_PART_WEEK_DAY AS PLAN_YYMMDD
              ,ROW_NUMBER() OVER(ORDER BY T2.START_PART_WEEK_DAY) AS TIME_INDEX
              ,SALES_MGMT_VRSN_ID
          FROM M4S_O201010 T1
              ,M4S_I002030 T2
         WHERE DP_VRSN_ID = 'SP_2021W41.01'
           AND T2.YYMMDD BETWEEN T1.PLAN_FROM_YYMMDD AND T1.PLAN_TO_YYMMDD
         GROUP BY T2.START_PART_WEEK_DAY,SALES_MGMT_VRSN_ID ) T2
      ,VIEW_I002040 T3
      ,M4S_I204030 T4
 WHERE T1.PLAN_YYMMDD = T2.PLAN_YYMMDD
   AND T1.ITEM_CD = T3.ITEM_CD
   AND T1.SALES_MGMT_CD = T4.SALES_MGMT_CD
   AND T2.SALES_MGMT_VRSN_ID = T4.SALES_MGMT_VRSN_ID  
 GROUP BY T1.DP_VRSN_ID
         ,T1.SALES_MGMT_CD
         ,T4.SALES_MGMT_NM
         ,T1.ITEM_CD
         ,T1.USER_CD
         ,T1.VALUE_NAME
         ,T3.ITEM_NM
         ,T3.ITEM_ATTR01_CD
         ,T3.ITEM_ATTR01_NM
         ,T3.ITEM_ATTR02_CD
         ,T3.ITEM_ATTR02_NM
         ,T3.ITEM_ATTR03_CD
         ,T3.ITEM_ATTR03_NM
         ,T3.ITEM_ATTR04_CD
         ,T3.ITEM_ATTR04_NM
         ,T3.ITEM_ATTR05_CD
   ORDER BY 22,23,26