package day12;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class demo02_WordCount_sql_newAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, String>> tp = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String value) throws Exception {

                String[] fields = value.split(",");
                String aid = fields[0];
                String event = fields[1];
                String uid = fields[2];
                return Tuple3.of(aid, event, uid);

            }
        });

        tEnv.createTemporaryView("ADCont",tp,"aid,event,uid");

        //TableResult tableResult0 = tEnv.executeSql(" select aid, event, distinct uid d_uid from ADCont group by aid,event");
        TableResult tableResult1 = tEnv.executeSql("select aid, event, count(uid), count (distinct uid) aaa from ADCont group by aid,event");


        tableResult1.print();
       // tableResult0.print();
        env.execute();
    }
}
