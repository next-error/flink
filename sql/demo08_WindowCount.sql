--在滚动窗口中,将商品ID和行为进行聚合

CREATE TABLE KafkaTable (
                            `ts` BIGINT, -- 精确到毫秒
                            `user_id` BIGINT,
                            `item_id` BIGINT,
                            `behavior` STRING,
                            `ts2` as TO_TIMESTAMP_LTZ(ts,3) ,
                            WATERMARK FOR `ts2` AS ts2 - INTERVAL '0' SECOND -- 指定使用哪个字段作为EventTime，并指定延迟时间
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'linux01:9092,linux02:9092,linux03:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'latest-offset',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      )
;

--使用滚动窗口查询,按照ItemID behavior 分组

select
  item_id,
  behavior,
  count(*) counts
from
    KafkaTable
where item_id is not null
group by
    tumnle(ts2,INTERVAL '10' SECOND),item_id, behavior
;

--与历史数据聚合

select
    item_id,
    behavior,
    sum(counts) counts
from
    (
        select
            item_id,
            behavior,
            count(*) counts
        from
            KafkaTable
        where item_id is not null
        group by
            tumnle(ts2,INTERVAL '10' SECOND),item_id, behavior
        )
group by item_id, behavior