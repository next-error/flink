package cn.doitedu.day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 使用FlinkSQL实现流式的WordCount
 * 使用老的API注册视图
 */
public class C03_StreamSQLWordCount {

    public static void main(String[] args) throws Exception {

        //StreamExecutionEnvironment只能创建DataStream，并且调用DataStream的API
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //现在向使用SQL，使用StreamTableEnvironment将原来的StreamExecutionEnvironment增强
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //spark,1
        //hive,1
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //对数据流进行整理
        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });


        //使用tableEnv将DataStream关联schema，注册成视图
        tEnv.createTemporaryView("v_wc", tpStream);

        /**
         * +------+--------+-------+-----+--------+-----------+
         * | name |   type |  null | key | extras | watermark |
         * +------+--------+-------+-----+--------+-----------+
         * |   f0 | STRING |  true |     |        |           |
         * |   f1 |    INT | false |     |        |           |
         * +------+--------+-------+-----+--------+-----------+
         */

        TableResult tableResult = tEnv.executeSql("desc v_wc");

//        //写SQL
//        TableResult tableResult = tEnv.executeSql("select word, sum(counts) total_counts from v_wc group by word");
//
        tableResult.print();

        env.execute();
    }
}
