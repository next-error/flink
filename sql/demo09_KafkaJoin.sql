--从kafka中读取数据,糖后和mysql关联
--创建Kafka的source表
CREATE TABLE KafkaTable (
                            `oid` BIGINT, -- 精确到毫秒
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

--创建mysql的维度表
CREATE TABLE tb_category (
                          `category_id` BIGINT,
                           `name` STRING,
                          PRIMARY KEY (user_id) NOT ENFORCED -- 将user_id作为主键，如果主键重复的数据，会进行更新
) WITH (
      'connector' = 'jdbc',
      'url' = 'jdbc:mysql://linux01:3306/test?characterEncoding=utf-8',
      'table-name' = 'tb_category', -- 写入的mysql数据库对应的表
      'username' = 'root',
      'password' = 'root',
      'lookup.cache.max-rows' = '5000', --缓存数据的最大行数
      'lookup.cache.ttl' = '10min'   --设置缓存存活时间
      );

--写sql 将两个表进行join

SELECT
    e.oid,
    e.cid,
    e.money,
    c.name
FROM
    KafkaTable e
LEFT JOIN
    tb_category  FOR SYSTEM_TIME AS OF e.ts  as c --按照事实表中的ts设置缓存的ttl 表的别名要放在最后
ON c.cid = c.category_id;
