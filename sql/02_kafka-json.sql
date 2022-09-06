-- 从Kafka中读取json的数据
-- 获取一些kafka中的metadata字段

CREATE TABLE tb_events (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL, -- VIRTUAL 代表是虚拟字段，不是原本数据中的字段，而是从kafka的元数据信息中活获得的
  `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  -- from Kafka connector
  `offset` BIGINT METADATA FROM 'offset' VIRTUAL  -- 如果定义的字段名称和元数据中的字段名称一样，可以不写 from 'offset'
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
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP(3),
  `partition_id` BIGINT,
  `offset` BIGINT
) WITH (
  'connector' = 'print'
)
;
-- 从Kafka的Source表中查询数据并过滤，然后出入到Sink表中
INSERT INTO print_table SELECT * FROM tb_events WHERE user_id IS NOT NULL AND behavior <> 'pay'



