package day14;

import org.apache.commons.io.FileUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;

/**
 * 使用 sql 创建Kafka的Source ,关联维度表数据
 */
public class demo02_MySQLDim {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //u01,i101,view
        //执行SQL创建一个Source表

        String line = FileUtils.readFileToString(new File(args[0]));
        String[] sqls = line.split(";");
/*        for (String sql : sqls) {
            tEnv.executeSql(sql);
        }*/
        tEnv.executeSql(sqls[0]);
        //创建维度表
        tEnv.executeSql(sqls[1]);
        TableResult tableResult = tEnv.executeSql(sqls[2]);
        tableResult.print();


        env.execute();


    }
}
