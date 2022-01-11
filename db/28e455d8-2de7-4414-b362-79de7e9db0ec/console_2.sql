DECLARE @SC_RESULT_A TABLE
(
    ROWNUM INT,
    RESNUM INT,
    RES_CD VARCHAR(20),
    PLANT_CD VARCHAR(20),
    ITEM_CD VARCHAR(20),
    START_TIME DECIMAL(18,3),
    END_TIME DECIMAL(18,3),
    PROD_QTY DECIMAL(18,3)
)
DECLARE @SC_RESULT_B TABLE
(
    ROWNUM INT,
    RESNUM INT,
    RES_CD VARCHAR(20),
    PLANT_CD VARCHAR(20),
    ITEM_CD VARCHAR(20),
    START_TIME DECIMAL(18,3),
    END_TIME DECIMAL(18,3),
    PROD_QTY DECIMAL(18,3),
    CAPA_RATE DECIMAL(10,3)
)

DECLARE @SC_RESULT_C TABLE
(
    ROWNUM INT,
    RESNUM INT,
    RES_CD VARCHAR(20),
    PLANT_CD VARCHAR(20),
    ITEM_CD VARCHAR(20),
    START_TIME DECIMAL(18,3),
    END_TIME DECIMAL(18,3),
    PROD_QTY DECIMAL(18,3),
    YYMMDD VARCHAR(8),
    ST_DT VARCHAR(5),
    EN_DT VARCHAR(5)
)

INSERT @SC_RESULT_A
SELECT ROW_NUMBER() OVER(PARTITION BY T1.ROUTE_CD ORDER BY T1.ROUTE_CD,START_TIME) ROWNUM
      ,DENSE_RANK() OVER( ORDER BY T1.ROUTE_CD) RESNUM
      ,T3.RES_CD
      ,T3.PLANT_CD
      ,T3.ITEM_CD
      ,ROUND(T1.START_TIME,0)
      ,ROUND(T1.START_TIME + T2.PROD_QTY * T3.CAPA_USE_RATE,0) AS END_TIME
      ,T2.PROD_QTY
  fROM M4E_O402110 T1
      ,M4E_O402120 T2
      ,M4S_I305110 T3
WHERE T1.ROUTE_CD = T2.ROUTE_CD
   AND T1.ENG_ITEM_CD = T2.ENG_ITEM_CD
   AND PROD_QTY > 0
   AND T1.ROUTE_CD = T3.RES_CD+'@'+T3.PLANT_CD
   AND T1.ENG_ITEM_CD = T3.ITEM_CD+'@'+T3.PLANT_CD
   AND T3.USE_YN = 'Y'

INSERT @SC_RESULT_B
SELECT NULL
      ,NULL
      ,T3.RES_CD
      ,T3.PLANT_CD
      ,T3.ITEM_CD
      ,ROUND(T1.START_TIME,0)
      ,ROUND(T1.START_TIME + T2.PROD_QTY * T3.CAPA_USE_RATE,0) AS END_TIME
      ,T2.PROD_QTY
      ,T3.CAPA_USE_RATE
  fROM M4E_O402110 T1
      ,M4E_O402120 T2
      ,M4S_I305110 T3
WHERE T1.ROUTE_CD = T2.ROUTE_CD
   AND T1.ENG_ITEM_CD = T2.ENG_ITEM_CD
   AND PROD_QTY > 0
   AND T1.ROUTE_CD = T3.RES_CD+'@'+T3.PLANT_CD
   AND T1.ENG_ITEM_CD = T3.ITEM_CD+'@'+T3.PLANT_CD
   AND T3.USE_YN = 'Y'

DECLARE @ROW_CNT INT;
DECLARE @RES_CNT INT;
DECLARE @MAX_ROW INT;
DECLARE @MAX_RES INT;
DECLARE @TIME    INT;
DECLARE @START_YYMMDD VARCHAR(8);
DECLARE @YYMMDD       VARCHAR(8);
DECLARE @DAYOVER INT;

-- 계획 시작일
SELECT @START_YYMMDD = FROM_YYMMDD
  FROM M4E_I401010;

-- 설비대수
SELECT @MAX_RES = MAX(RESNUM)
  FROM @SC_RESULT_A;


SET @RES_CNT = 1
WHILE @RES_CNT <= @MAX_RES
    BEGIN
        -- 설비별 Row Count
        SELECT @MAX_ROW = MAX(ROWNUM)
          FROM @SC_RESULT_A
         WHERE RESNUM = @RES_CNT;

        SET @ROW_CNT = 1
        WHILE @ROW_CNT <= @MAX_ROW
            BEGIN
                IF @ROW_CNT < @MAX_ROW
                    BEGIN
                        INSERT INTO @SC_RESULT_B
                        SELECT NULL
                              ,NULL
                              ,T1.RES_CD
                              ,T1.PLANT_CD
                              ,'J/C' AS ITEM_CD
                              ,T1.END_TIME
                              ,T2.START_TIME
                              ,0
                              ,0
                          FROM @SC_RESULT_A T1
                              ,@SC_RESULT_A T2
                         WHERE T1.RESNUM = @RES_CNT
                           AND T1.ROWNUM = @ROW_CNT
                           AND T2.RESNUM = @RES_CNT
                           AND T2.ROWNUM = @ROW_CNT + 1
                    END
                SET @ROW_CNT = @ROW_CNT + 1
            END
        SET @RES_CNT = @RES_CNT + 1
    END

UPDATE T1
   SET T1.ROWNUM = T2.ROWNUM
      ,T1.RESNUM = T2.RESNUM
  FROM @SC_RESULT_B T1
  JOIN (SELECT ROW_NUMBER() OVER(PARTITION BY RES_CD ORDER BY RES_CD,START_TIME) ROWNUM
              ,DENSE_RANK() OVER(ORDER BY RES_CD) RESNUM
              ,RES_CD
              ,PLANT_CD
              ,ITEM_CD
              ,START_TIME
          FROM @SC_RESULT_B) T2
    ON  T1.RES_CD = T2.RES_CD
    AND T1.PLANT_CD =T2.PLANT_CD
    AND T1.ITEM_CD = T2.ITEM_CD
    AND T1.START_TIME = T2.START_TIME;


SELECT * FROM @SC_RESULT_B
ORDER BY 2,1

SELECT @MAX_RES = MAX(RESNUM)
  FROM @SC_RESULT_B;

INSERT INTO @SC_RESULT_C
SELECT NULL
      ,NULL
      ,RES_CD
      ,PLANT_CD
      ,ITEM_CD
      ,START_TIME
      ,END_TIME
      ,PROD_QTY
      ,NULL
      ,NULL
      ,NULL
  FROM @SC_RESULT_B;

SET @RES_CNT = 1
WHILE @RES_CNT <= @MAX_RES
    BEGIN
        -- 날짜/시간 초기화
        SET @TIME = 0;
        SET @YYMMDD = @START_YYMMDD;

        SELECT @MAX_ROW = MAX(ROWNUM)
          FROM @SC_RESULT_B
         WHERE RESNUM = @RES_CNT;

        SET @ROW_CNT = 1
        WHILE @ROW_CNT <= @MAX_ROW
            BEGIN

                SELECT @DAYOVER = (END_TIME/1440) - (START_TIME/1440)
                  FROM @SC_RESULT_B
                 WHERE RESNUM = @RES_CNT
                   AND ROWNUM = @ROW_CNT;

                 IF @DAYOVER = 0
                    BEGIN
                        UPDATE T1
                            SET T1.YYMMDD = @YYMMDD
                                ,T1.ST_DT = CASE WHEN LEN(((T2.START_TIME/1440)*24) - (T2.START_TIME/60)) = 1 THEN '0' ELSE '' END
                                            + CAST(((T2.START_TIME/1440)*24) - (T2.START_TIME/60) AS VARCHAR) +':'
                                            + CASE WHEN LEN(T2.START_TIME%60) = 1 THEN '0' ELSE '' END
                                            + CAST(T2.START_TIME%60 AS VARCHAR)
                                ,T1.EN_DT = CASE WHEN LEN(((T2.END_TIME/1440)*24) - (T2.END_TIME/60)) = 1 THEN '0' ELSE '' END
                                            + CAST(((T2.END_TIME/1440)*24) - (T2.END_TIME/60) AS VARCHAR) +':'
                                            + CASE WHEN LEN(T2.END_TIME%60) = 1 THEN '0' ELSE '' END
                                            + CAST(T2.END_TIME%60 AS VARCHAR)
                            FROM @SC_RESULT_C T1
                            JOIN @SC_RESULT_B T2
                            ON T1.RES_CD = T2.RES_CD
                            AND T1.PLANT_CD = T2.PLANT_CD
                            AND T1.ITEM_CD = T2.ITEM_CD
                            AND T1.START_TIME = T2.START_TIME
                            AND T2.RESNUM = @RES_CNT
                            AND T2.ROWNUM = @ROW_CNT;
                    END
                ELSE
                    BEGIN

                    -- 여기서부터 해야함.
                    -- 날짜가 넘어가는 ROW는 날짜별로 분리 필요.
                        UPDATE T1
                            SET T1.YYMMDD = @YYMMDD
                                ,T1.ST_DT = '00:00'
                                ,T1.EN_DT = '24:00'
                                ,T1.PROD_QTY =
                            FROM @SC_RESULT_C T1
                            JOIN @SC_RESULT_B T2
                            ON T1.RES_CD = T2.RES_CD
                            AND T1.PLANT_CD = T2.PLANT_CD
                            AND T1.ITEM_CD = T2.ITEM_CD
                            AND T1.START_TIME = T2.START_TIME
                            AND T2.RESNUM = @RES_CNT
                            AND T2.ROWNUM = @ROW_CNT;
                    END

                SET @ROW_CNT = @ROW_CNT + 1
            END
        SET @RES_CNT = @RES_CNT + 1
    END
