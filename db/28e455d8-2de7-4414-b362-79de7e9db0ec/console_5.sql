SELECT * FROM M4S_I002140


SELECT * FROM M4S_I002011 WHERE COMM_CD = 'CURCY_CD'

select LINK_SALES_MGMT_CD
    , USER_CD
from m4s_i204040
where 1=1
  AND SALES_MGMT_VRSN_ID='202111_V0'
  AND SALES_MGMT_TYPE_CD = 'SP1'



select *
from m4s_i204020
where 1=1
  AND SALES_MGMT_VRSN_ID='202111_V0'


select SALES_MGMT_CD
     , USER_CD
from m4s_o201001
where DP_VRSN_ID = 'SP_2021W49.01'

select * from m4s_o201020
where DP_VRSN_ID = 'SP_2021W49.01'
SELECT T1.PROJECT_CD
                     ,T1.SALES_MGMT_VRSN_ID
                     ,T3.SALES_MGMT_TYPE_CD
                     ,T1.LINK_SALES_MGMT_CD
                     ,T1.USER_CD
                 FROM M4S_I204040 T1
                     ,M4S_O201010 T2
                     ,M4S_I204030 T3
                WHERE 1=1
                  AND T2.PROJECT_CD = :PROJECT_CD
                  AND T1.SALES_MGMT_VRSN_ID = T2.SALES_MGMT_VRSN_ID
                  AND T2.DP_VRSN_ID = :SP_VRSN_ID
                  AND SUBSTRING(T1.LINK_SALES_MGMT_CD,5,3) = :SP1_CD
                  AND T3.SALES_MGMT_TYPE_CD = 'SP1_C'
                  AND T1.PROJECT_CD = T3.PROJECT_CD
                  AND T1.PROJECT_CD = T2.PROJECT_CD
                  AND T1.SALES_MGMT_VRSN_ID = T3.SALES_MGMT_VRSN_ID
                  AND T1.LINK_SALES_MGMT_CD = T3.SALES_MGMT_CD
                GROUP BY T1.PROJECT_CD
                        ,T1.SALES_MGMT_VRSN_ID
                        ,T3.SALES_MGMT_TYPE_CD
                        ,T1.LINK_SALES_MGMT_CD
                        ,T1.USER_CD


SELECT SALES_MGMT_CD
FROM M4S_I204030
WHERE 1=1
  AND PROJECT_CD = :VS_PROJECT_CD
  AND SALES_MGMT_VRSN_ID = '202111_V0'

SELECT * FROM M4S_O201001 WHERE DP_VRSN_ID = 'SP_2021W49.01' ORDER BY MODIFY_DATE DESC

SELECT * FROM M4S_O201003

SELECT
FROM M4S_I204040
WHERE PROJECT_CD = 'ENT001'
  AND SALES_MGMT_VRSN_ID = '202111_V0'

SELECT
FROM M4S_I204050
WHERE 1=1
  AND PROJECT_CD = 'ENT001'
  AND SALES_MGMT_VRSN_ID = '202111_V0'
  AND USER_CD = :VS_USER_CD


SELECT T1.DP_PRICE = CASE :VS_PRICE WHEN 35 THEN T2.CON_PRICE
                                    WHEN 36 THEN T2.FAC_PRICE
                                    WHEN 37 THEN T2.SHP_PRICE
                                    WHEN 38 THEN T2.AGN_PRICE
                                    END PRICE
FROM M4S_O201001 T1
  LEFT JOIN M4S_I002041 T2
  ON 1=1
  AND T1.PROJECT_CD = T2.PROJECT_CD
  AND T1.ITEM_CD = T2.ITEM_CD
  AND T2.PRICE_QTY_UNIT_CD = 'BOX'
WHERE T1.PROJECT_CD = 'ENT001'
  AND T1.DP_VRSN_ID = 'SP_2021W49.01'


SELECT CASE :VS_PRICE WHEN 35 THEN T2.CON_PRICE
                                               WHEN 36 THEN T2.FAC_PRICE
                                               WHEN 37 THEN T2.SHP_PRICE
                                               WHEN 38 THEN T2.AGN_PRICE
                                               END
                 ,:USER_CD
                 ,GETDATE()
, T1.SALES_MGMT_CD
           FROM M4S_O201001 T1
         LEFT JOIN M4S_I002041 T2
             ON 1=1
             AND T1.PROJECT_CD = T2.PROJECT_CD
             AND T1.ITEM_CD = T2.ITEM_CD
             AND T2.PRICE_QTY_UNIT_CD = 'BOX'
           WHERE T1.PROJECT_CD = :PROJECT_CD
             AND T1.DP_VRSN_ID = :SP_VRSN_ID
             AND T1.USER_CD = :USER_CD
             AND SUBSTRING(T1.SALES_MGMT_CD,1,2) = :SP1_C_CD
             AND SUBSTRING(T1.SALES_MGMT_CD,8,4) = :SP1_CD

exec PRC_SP1_INSERT 'ENT001','11_1012',38,'dev07','SP_2021W49.01';


UPDATE T1
             SET T1.DP_PRICE = CASE :VS_PRICE WHEN 35 THEN T2.CON_PRICE
                                               WHEN 36 THEN T2.FAC_PRICE
                                               WHEN 37 THEN T2.SHP_PRICE
                                               WHEN 38 THEN T2.AGN_PRICE
                                               END
                 ,T1.MODIFY_USER_CD = :USER_CD
                 ,T1.MODIFY_DATE = GETDATE()
           FROM M4S_O201001 T1
         LEFT JOIN M4S_I002041 T2
             ON 1=1
             AND T1.PROJECT_CD = T2.PROJECT_CD
             AND T1.ITEM_CD = T2.ITEM_CD
             AND T2.PRICE_QTY_UNIT_CD = 'BOX'
           WHERE T1.PROJECT_CD = :PROJECT_CD
             AND T1.DP_VRSN_ID = :SP_VRSN_ID
             AND T1.USER_CD = :USER_CD
             AND SUBSTRING(T1.SALES_MGMT_CD,1,2) = :SP1_C_CD
             AND SUBSTRING(T1.SALES_MGMT_CD,8,4) = :SP1_CD
