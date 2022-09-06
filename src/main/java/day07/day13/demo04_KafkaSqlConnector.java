package day07.day13;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 使用 sql 创建Kafka的Source ,从Kafka读数据
 */
public class demo04_KafkaSqlConnector {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //u01,i101,view
        //执行SQL创建一个Source表


        tEnv.executeSql("CREATE TABLE KafkaTable (\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp' \n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'user_behavior',\n" +
                "  'properties.bootstrap.servers' = 'linux01:9092,linux02:9092,linux03:9092',\n" +
                "  'properties.group.id' = 'g001',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'csv'\n" +
                "  'csv.ignore-parse-errors' = 'true' \n" +  //忽略解析出错的数据，对应的字段为NULL
                ")");
        TableResult tableResult = tEnv.executeSql("select user_id from KafkaTable");
        //TableResult tableResult = tEnv.executeSql("desc KafkaTable");
        tableResult.print();

        env.execute();


    }
}
