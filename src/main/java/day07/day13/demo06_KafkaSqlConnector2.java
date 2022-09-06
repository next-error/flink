package day07.day13;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 使用 sql 创建Kafka的Source ,从Kafka读数据
 */
public class demo06_KafkaSqlConnector2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //u01,i101,view
        //执行SQL创建一个Source表

        String line = FileUtils.readFileToString(new File(args[0]));
        String[] sqls = line.split(";");
        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }



        env.execute();


    }
}
