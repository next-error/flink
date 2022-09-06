package day07.day13;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.http.conn.scheme.Scheme;

/**
 * 使用Schema
 */
public class demo01_schema {

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

        Schema schema = Schema.newBuilder()
                .column("f0", DataTypes.STRING()) //给列指定类型
                .column("f1",DataTypes.INT())
                .columnByExpression("word","f0") //相当于字段起别名,但会得到新的列
                .columnByExpression("count","f1")
                .columnByExpression("counts","cast (f1 as bigint)") //counts 类型为bigint
                //从元数据中获取字段,将processingTime转换为 TIMESTAMP_LTZ 并命名为ptime
                .columnByMetadata("ptime",DataTypes.TIMESTAMP_LTZ(3),"proctime")
                .build();
        //使用tableEnv将DataStream关联schema，注册成视图
        //tEnv.createTemporaryView("v_wc", tpStream, "word,counts");
        tEnv.createTemporaryView("v_tb",tpStream,schema);

        //写SQL
        TableResult tableResult = tEnv.executeSql("desc v_tb");

        //TableResult tableResult = tEnv.executeSql("select word, sum(counts) total_counts from v_wc group by word");

        tableResult.print();

        env.execute();
    }
}
