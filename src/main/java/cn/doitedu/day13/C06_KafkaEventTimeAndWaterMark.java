package cn.doitedu.day13;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 使用SQL方法，创建Kafka的Source，从Kafka中读取数据
 */
public class C06_KafkaEventTimeAndWaterMark {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //读取SQL文件中的内容
        //String line = FileUtils.readFileToString(new File(args[0]), "UTF-8");
        //String[] sqls = SQLHolder.split(line);
        //tEnv.executeSql(sqls[1]);

        tEnv.executeSql("CREATE TABLE tb_events (\n" +
                "  `ts` BIGINT, -- 精确到毫秒\n" +
                "  `user_id` BIGINT,\n" +
                "  `item_id` BIGINT,\n" +
                "  `behavior` STRING,\n" +
                "  `ts2` as TO_TIMESTAMP(FROM_UNIXTIME(ts  / 1000)), -- 将类型的时间，转成 yyyy-MM-dd HH:mm:ss\n" +
                "  WATERMARK FOR `ts2` AS ts2 - INTERVAL '0' SECOND -- 指定使用哪个字段作为EventTime，并指定延迟时间\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'tp-events2',\n" +
                "  'properties.bootstrap.servers' = 'node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")");


        TableResult tableResult = tEnv.executeSql("select *, CURRENT_WATERMARK(ts2) from tb_events where user_id is not null");

        tableResult.print();




    }
}
