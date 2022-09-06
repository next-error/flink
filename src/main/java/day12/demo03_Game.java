package day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Random;
import java.util.UUID;

public class demo03_Game {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple4<String, String, Integer,Integer>> tp = lines.map(new MapFunction<String, Tuple4<String, String, Integer,Integer>>() {
            @Override
            public Tuple4<String, String, Integer,Integer> map(String value) throws Exception {

                String[] fields = value.split(",");
                String gid = fields[0];
                String uid = fields[1];
                Integer gold = Integer.parseInt(fields[2]);
                return Tuple4.of(gid, uid, gold, 1);

            }
        });

        tEnv.createTemporaryView("game",tp,"gid,uid,gold,r1");

        //TableResult tableResult0 = tEnv.executeSql(" select aid, event, distinct uid d_uid from ADCont group by aid,event");
        TableResult tableResult1 = tEnv.executeSql("");


        tableResult1.print();
       // tableResult0.print();
        env.execute();
    }
}
