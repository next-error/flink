-- 从Kafka中读取json的数据
-- 并且提取数据中所携带的时间作为eventTime生成WaterMark
-- TIMESTAMP(3) 2022-07-07 15:11:30.001
-- TIMESTAMP(1) 2022-07-07 15:11:30
CREATE TABLE tb_events (
  `user_id` BIGINT,
  `ts` TIMESTAMP(3), -- 对应的是精确到毫秒的字符串类型 TIMESTAMP(3) 2022-07-07 15:11:30.001
  `item_id` BIGINT,
  `behavior` STRING,
   WATERMARK FOR `ts` AS ts - INTERVAL '0' SECOND -- 指定使用哪个字段作为EventTime，并指定延迟时间
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
  `user_id` BIGINT,
  `ts` TIMESTAMP(3),
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'print'
)
;
-- 从Kafka的Source表中查询数据并过滤，然后出入到Sink表中
INSERT INTO print_table SELECT * FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'



