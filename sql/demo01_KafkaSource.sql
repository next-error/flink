--从Kafka中读取数据
CREATE TABLE KafkaTable (
                            `user_id` BIGINT,
                            `item_id` BIGINT,
                            `behavior` STRING,
                            `ts` TIMESTAMP(3) METADATA FROM 'timestamp'
) WITH (
      'connector' = 'kafka',
      'topic' = 'user_behavior',
      'properties.bootstrap.servers' = 'linux01:9092,linux02:9092,linux03:9092',
      'properties.group.id' = 'testGroup',
      'scan.startup.mode' = 'earliest-offset',
      'format' = 'csv',
      'csv.ignore-parse-errors' = 'true'
      )
;
CREATE TABLE print_table (
                             `user_id` BIGINT,
                             `item_id` BIGINT,
                             `behavior` STRING,
                             `ts` TIMESTAMP(3)
) WITH (
      'connector' = 'print'
      )
;

INSERT INTO print_table SELECT * FROM KafkaTable WHERE user_id IS NOT NULL AND behavior <> 'pay'