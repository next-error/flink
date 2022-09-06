package day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class demo05_SqlTumblingEventWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple4<Long, String, String,Integer>> tp = lines.map(new MapFunction<String, Tuple4<Long, String, String,Integer>>() {
            @Override
            public Tuple4<Long, String, String,Integer> map(String value) throws Exception {

                String[] fields = value.split(",");
                Long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                Integer money = Integer.parseInt(fields[3]);
                return Tuple4.of(time, uid, pid, money);

            }
        });
        tEnv.createTemporaryView("game",tp,"time,uid,pid,money");


        TableResult tableResult = tEnv.executeSql("select uid from game");

        tableResult.print();
        env.execute();
    }
}
