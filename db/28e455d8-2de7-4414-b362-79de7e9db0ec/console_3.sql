CREATE OR ALTER PROCEDURE [dbo].[PRC_FP_JOB_CHANGE_211110]
(
    @V_PROJECT_CD       VARCHAR(50)
 )
AS
    DECLARE @V_PROC_NM    VARCHAR(50); -- 프로시저이름
BEGIN
    SET @V_PROC_NM = 'PRC_FP_JOB_CHANGE_211110';
    exec dbo.MTX_SCM_PROC_LOG @V_PROJECT_CD, @V_PROC_NM,
        'PRC_FP_JOB_CHANGE_211110 프로시저', 'ALL START';

    DELETE FROM TMP_M4E_O402130 WHERE 1=1;


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
          ,ROUND(T1.START_TIME,0) AS START_TIME
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
          ,ROUND(T1.START_TIME,0) AS START_TIME
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
    DECLARE @PROD DECIMAL(18,3);
    DECLARE @START INT;
    DECLARE @END INT;
    DECLARE @DAYOVER_CNT INT;
    DECLARE @START_DAY INT;
    DECLARE @DAY_CNT INT;

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


    -- SELECT * FROM @SC_RESULT_B
    -- ORDER BY 2,1

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

                    SELECT @DAYOVER = (CAST(END_TIME AS INT)/1440) - (CAST(START_TIME AS INT)/1440)
                         , @START_DAY = CAST(START_TIME AS INT)/1440
                         , @DAYOVER_CNT = CAST(END_TIME AS INT)/1440
                      FROM @SC_RESULT_B
                     WHERE RESNUM = @RES_CNT
                       AND ROWNUM = @ROW_CNT;

                     IF @DAYOVER = 0
                        BEGIN
    --                             SELECT T2.END_TIME
    --                             FROM @SC_RESULT_C T1
    --                             JOIN @SC_RESULT_B T2
    --                             ON T1.RES_CD = T2.RES_CD
    --                             AND T1.PLANT_CD = T2.PLANT_CD
    --                             AND T1.ITEM_CD = T2.ITEM_CD
    --                             AND T1.START_TIME = T2.START_TIME
    --                             AND T2.RESNUM = @RES_CNT
    --                             AND T2.ROWNUM = @ROW_CNT;
    --                         SELECT @YYMMDD
    --                              , T2.START_TIME
    --                              ,((CAST(T2.START_TIME AS INT)/1440)*24) - (CAST(T2.START_TIME AS INT)/60)
    --                                 ,LEN(((T2.START_TIME/1440)*24) - (T2.START_TIME/60))
    --                                 ,(T2.START_TIME/1440)*24
    --                                , (T2.START_TIME/60) +':'
    --                                ,LEN(T2.START_TIME%60)
    --                                ,T2.START_TIME%60
    --                                 ,LEN(((T2.END_TIME/1440)*24) - (T2.END_TIME/60))
    --                                 ,((T2.END_TIME/1440)*24)
    --                                      , (T2.END_TIME/60) +':'
    --                                 ,LEN(T2.END_TIME%60)
    --                                 ,T2.END_TIME%60
    --                             FROM @SC_RESULT_C T1
    --                             JOIN @SC_RESULT_B T2
    --                             ON T1.RES_CD = T2.RES_CD
    --                             AND T1.PLANT_CD = T2.PLANT_CD
    --                             AND T1.ITEM_CD = T2.ITEM_CD
    --                             AND T1.START_TIME = T2.START_TIME
    --                             AND T2.RESNUM = @RES_CNT
    --                             AND T2.ROWNUM = @ROW_CNT;
                            UPDATE T1
                                SET T1.YYMMDD = FORMAT(DATEADD(DAY, @START_DAY, @YYMMDD),'yyyyMMdd')
                                    ,T1.START_TIME = T2.START_TIME  - 1440 * @START_DAY
                                    ,T1.END_TIME = T2.END_TIME - 1440 * @START_DAY
                                    ,T1.ST_DT = CASE WHEN LEN((CAST(T2.START_TIME AS INT) - 1440 * @START_DAY)/60) = 1 THEN '0' ELSE '' END
                                            + CAST((CAST(T2.START_TIME AS INT) - 1440 * @START_DAY)/60 AS VARCHAR) +':'
                                            + CASE WHEN LEN((CAST(T2.START_TIME AS INT) - 1440 * @START_DAY)%60) = 1 THEN '0' ELSE '' END
                                            + CAST(FORMAT((CAST(T2.START_TIME AS INT) - 1440 * @START_DAY)%60,'#0') AS VARCHAR)
                                    ,T1.EN_DT = CASE WHEN LEN((CAST(T2.END_TIME AS INT) - 1440 * @START_DAY)/60) = 1 THEN '0' ELSE '' END
                                            + CAST((CAST(T2.END_TIME AS INT) - 1440 * @START_DAY)/60 AS VARCHAR) +':'
                                            + CASE WHEN LEN((CAST(T2.END_TIME AS INT) - 1440 * @START_DAY)%60) = 1 THEN '0' ELSE '' END
                                            + CAST(FORMAT((CAST(T2.END_TIME AS INT) - 1440 * @START_DAY)%60,'#0') AS VARCHAR)
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

                            SELECT @PROD = PROD_QTY--ROUND(PROD_QTY * (((1440 * @DAYOVER_CNT) - START_TIME) / (END_TIME- START_TIME)),0)
                                , @START = CAST(START_TIME AS INT)
                                , @END = CAST(END_TIME AS INT)
                              FROM @SC_RESULT_B
                             WHERE RESNUM = @RES_CNT
                               AND ROWNUM = @ROW_CNT;


                        -- 여기서부터 해야함.
                        -- 날짜가 넘어가는 ROW는 날짜별로 분리 필요.
                            SET @DAY_CNT = @DAYOVER
                            WHILE @DAY_CNT >= 0
                                BEGIN
                                    IF @DAY_CNT = @DAYOVER_CNT -- 마지막부분
                                        BEGIN
                                            -- 마지막 부분
                                            INSERT INTO @SC_RESULT_C
                                            SELECT T1.ROWNUM
                                                , T1.RESNUM
                                                , T1.RES_CD
                                                , T1.PLANT_CD
                                                , T1.ITEM_CD
                                                , 0
                                                , T1.END_TIME - (1440 * @DAYOVER_CNT)
                                                , (@PROD * CAST(@END - (1440 * @DAYOVER_CNT) AS DECIMAL(18,3)) / (@END- @START))
                                                , FORMAT(DATEADD(DAY, @DAYOVER_CNT, @YYMMDD), 'yyyyMMdd')
                                                , '00:00'
                                                , CASE WHEN LEN((CAST(T2.END_TIME AS INT) - (1440 * @DAYOVER_CNT)) /60) = 1 THEN '0' ELSE '' END
                                                            + CAST((CAST(T2.END_TIME AS INT) - (1440 * @DAYOVER_CNT)) /60 AS VARCHAR) +':'
                                                            + CASE WHEN LEN((CAST(T2.END_TIME AS INT)-(1440 * @DAYOVER_CNT))%60) = 1 THEN '0' ELSE '' END
                                                            + CAST(FORMAT((CAST(T2.END_TIME AS INT)-(1440 * @DAYOVER_CNT))%60,'#0') AS VARCHAR)
                                            FROM @SC_RESULT_C T1
                                            JOIN @SC_RESULT_B T2
                                                ON T1.RES_CD = T2.RES_CD
                                                AND T1.PLANT_CD = T2.PLANT_CD
                                                AND T1.ITEM_CD = T2.ITEM_CD
                                                AND T1.START_TIME = T2.START_TIME
                                                AND T2.RESNUM = @RES_CNT
                                                AND T2.ROWNUM = @ROW_CNT;
                                        end
                                    ELSE
                                        BEGIN
                                            IF @DAY_CNT = 0 -- 처음부분
                                                BEGIN

                                                    -- 시작부분
                                                    UPDATE T1
                                                        SET T1.YYMMDD = FORMAT(DATEADD(DAY, @START_DAY, @YYMMDD), 'yyyyMMdd')
                                                            ,T1.END_TIME = 1440
                                                            ,T1.ST_DT = CASE WHEN LEN((CAST(T2.START_TIME AS INT) - (1440 * @START_DAY)) / 60) = 1 THEN '0' ELSE '' END
                                                                    + CAST((CAST(T2.START_TIME AS INT) - (1440 * @START_DAY)) / 60 AS VARCHAR) +':'
                                                                    + CASE WHEN LEN((CAST(T2.START_TIME AS INT) - (1440 * @START_DAY)) % 60) = 1 THEN '0' ELSE '' END
                                                                    + CAST(FORMAT((CAST(T2.START_TIME AS INT) - (1440 * @START_DAY)) % 60,'#0') AS VARCHAR)
                                                            ,T1.EN_DT = '24:00'
                                                            ,T1.PROD_QTY = @PROD * (CAST(((1440 * (@START_DAY + 1)) - @START) AS DECIMAL(18,3)) / (@END- @START))
                                                        FROM @SC_RESULT_C T1
                                                        JOIN @SC_RESULT_B T2
                                                        ON T1.RES_CD = T2.RES_CD
                                                        AND T1.PLANT_CD = T2.PLANT_CD
                                                        AND T1.ITEM_CD = T2.ITEM_CD
                                                        AND T1.START_TIME = T2.START_TIME
                                                        AND T2.RESNUM = @RES_CNT
                                                        AND T2.ROWNUM = @ROW_CNT;
                                                end
                                            ELSE
                                                BEGIN
                                                    INSERT INTO @SC_RESULT_C
                                                    SELECT T1.ROWNUM
                                                        , T1.RESNUM
                                                        , T1.RES_CD
                                                        , T1.PLANT_CD
                                                        , T1.ITEM_CD
                                                        , 0
                                                        , 1440
                                                        , @PROD * (1440.0 / (@END- @START))
                                                        , FORMAT(DATEADD(DAY, @START_DAY + @DAY_CNT, @YYMMDD), 'yyyyMMdd')
                                                        , '00:00'
                                                        , '24:00'
                                                    FROM @SC_RESULT_C T1
                                                    JOIN @SC_RESULT_B T2
                                                        ON T1.RES_CD = T2.RES_CD
                                                        AND T1.PLANT_CD = T2.PLANT_CD
                                                        AND T1.ITEM_CD = T2.ITEM_CD
                                                        AND T1.START_TIME = T2.START_TIME
                                                        AND T2.RESNUM = @RES_CNT
                                                        AND T2.ROWNUM = @ROW_CNT;
                                                end
                                        end
                                    SET @DAY_CNT = @DAY_CNT - 1
                                    --SELECT @DAY_CNT
                                end






                        END

                    SET @ROW_CNT = @ROW_CNT + 1
                END
            SET @RES_CNT = @RES_CNT + 1
        END


        UPDATE T1
        SET T1.ROWNUM   = T2.ROWNUM
            , T1.RESNUM = T2.RESNUM
        FROM @SC_RESULT_C T1
        JOIN (
            SELECT ROW_NUMBER() over (PARTITION BY RES_CD ORDER BY YYMMDD, START_TIME) ROWNUM
                 , DENSE_RANK() OVER (ORDER BY RES_CD) RESNUM
                 , RES_CD
                 , PLANT_CD
                 , ITEM_CD
                 , YYMMDD
                 , START_TIME
            FROM @SC_RESULT_C
            --ORDER BY RES_CD, YYMMDD, ST_DT
        ) T2
        ON      T1.RES_CD      = T2.RES_CD
            AND T1.PLANT_CD    = T2.PLANT_CD
            AND T1.ITEM_CD     = T2.ITEM_CD
            AND T1.YYMMDD      = T2.YYMMDD
            AND T1.START_TIME  = T2.START_TIME;

        INSERT INTO TMP_M4E_O402130
        SELECT *
        FROM @SC_RESULT_C
        ORDER BY RES_CD, YYMMDD, ST_DT

        SELECT * --INTO TMP_M4E_O402130
        FROM @SC_RESULT_C
            ORDER BY RES_CD, YYMMDD, ST_DT
END