-- 在FlinkSQL中，提取EventTime生成的WaterMark的必须是TIMESTAMP(3)类型
CREATE TABLE tb_events (
  `ts` BIGINT, -- 精确到毫秒
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts2` as TO_TIMESTAMP(FROM_UNIXTIME(ts  / 1000)), -- 将类型的时间，转成 yyyy-MM-dd HH:mm:ss
  WATERMARK FOR `ts2` AS ts2 - INTERVAL '0' SECOND -- 指定使用哪个字段作为EventTime，并指定延迟时间
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-events',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
;

-- 创建一个print的Sink标签
CREATE TABLE print_table (
  `ts` BIGINT,
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts2` TIMESTAMP(3),
  `current_watermark` TIMESTAMP(3)
) WITH (
  'connector' = 'print'
)
;
-- 从Kafka的Source表中查询数据并过滤，然后出入到Sink表中
INSERT INTO print_table SELECT ts, user_id, item_id, behavior, ts2, CURRENT_WATERMARK(ts2) FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'



