-- 不划分窗口,实时求topN
-- 按照商品分类求TopN, 关联维度数据
CREATE TABLE KafkaTable (
                            `pid` BIGINT, -- 精确到毫秒
                            `cid` BIGINT,
                            `money` DOUBLE ,
                            `ts` as PROCTIME()
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

-- 将数据进行聚合,并且划分


select
    cid,
    sum(money) total_money
from
    KafkaTable
group by cid;



select
    cid,
    pid,
    total_money,
    row_number() over(partition by total_money desc) routine_name rn
from
    (
        select
            cid,
            pid,
            sum(money) total_money
        from
            KafkaTable
        group by cid, pid
        );

select
    cid,
    pid,
    total_money,
    rn,
    PROCTIME() ts  -- 获取当前系统时间
from
    (
        select
            cid,
            pid,
            total_money,
            row_number() over(partition by total_money desc) routine_name rn
        from
            (
                select
                    cid,
                    pid,
                    sum(money) total_money
                from
                    KafkaTable
                group by cid,pid
            )
        )
where
    rn <=3 ;

--  创建临时视图
CREATE TEMPORARY VIEW v_trmp1 as
select
    cid,
    pid,
    total_money,
    rn,
    PROCTIME() ts  -- 获取当前系统时间
from
    (
        select
            cid,
            pid,
            total_money,
            row_number() over(partition by total_money desc) routine_name rn
        from
            (
                select
                    cid,
                    pid,
                    sum(money) total_money
                from
                    KafkaTable
                group by cid,pid
            )
    )
where
        rn <=3 ;

-- 从视图中查询
select

-----------------------------------------------------------------------------------



