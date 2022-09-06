-- windowTVF 演示,原来window的简写方式,并且做了扩展

CREATE TABLE KafkaTable (
                            `ts` BIGINT,
                            `pid` BIGINT, -- 精确到毫秒
                            `cid` BIGINT,
                            `money` DOUBLE ,
                            `ts2` as TO_TIMESTAMP_LTZ(ts,3)
)
    WITH (
        'connector' = 'kafka',
        'topic' = 'user_behavior',
        'properties.bootstrap.servers' = 'linux01:9092,linux02:9092,linux03:9092',
        'properties.group.id' = 'testGroup',
        'scan.startup.mode' = 'latest-offset',
        'format' = 'csv',
        'csv.ignore-parse-errors' = 'true'
        )
;



SELECT
    window_start,
    window_end,
    cid,
    sum(money) money
FROM
TABLE
    ( -- TABLE 表名   DESCRIPTOR:按照哪个字段划分窗口 窗口长度
        HOP(TABLE KafkaTable, DESCRIPTOR(ts2), INTERVAL '3' Second, INTERVAL '10' Second )
    )
group by
    window_start,window_end
;
