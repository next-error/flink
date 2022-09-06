-- 窗口的TopN
-- 先对窗口的数据聚合,再求TOpN

-- 从Kafka读数据
CREATE TABLE KafkaTable (
                           `ts` BIGINT,
                            `pid` BIGINT, -- 精确到毫秒
                            `cid` BIGINT,
                            `money` DOUBLE ,
                            `ts2` as TO_TEAMS
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

-- 将数据按照窗口聚合
SELECT
    tumnle_start(ts2,INTERVAL '10' SECOND) win_start,
    tumnle_end(ts2,INTERVAL '10' SECOND) win_end,
    pid,
    cid,
    sum(money) money
from
    KafkaTable
group by
    tumnle(ts2,INTERVAL '10' SECOND),
         pid,
         cid
;


select
    win_start,
    win_end,
    pid,
    cid,
    money,
    row_number() over(partition by win_start, win_end,cid order by money desc) rn
from
    (
        SELECT
            tumnle_start(ts2,INTERVAL '10' SECOND) win_start,
            tumnle_end(ts2,INTERVAL '10' SECOND) win_end,
            pid,
            cid,
            sum(money) money
        from
            KafkaTable
        group by
            tumnle(ts2,INTERVAL '10' SECOND),
            pid,
            cid
        )

