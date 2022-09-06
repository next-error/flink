-- 从Kafka中读取数据，然后将数据写入到MySQL中
CREATE TABLE tb_events (
  `ts` BIGINT, -- 精确到毫秒
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp-events2',
  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
;

-- 在 Flink SQL 中注册一张 MySQL Sink表 'tb_users'
CREATE TABLE tb_users (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
   'connector' = 'jdbc',
   'url' = 'jdbc:mysql://node-1.51doit.cn:3306/doit31?characterEncoding=utf-8',
   'table-name' = 'tb_users3', -- 写入的mysql数据库对应的表
   'username' = 'root',
   'password' = '123456'
);

-- 从Kafka的Source表中查询数据并过滤，然后出入到Sink表中
INSERT INTO tb_users SELECT user_id, item_id, behavior FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'



