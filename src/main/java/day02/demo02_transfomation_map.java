package day02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Locale;


public class demo02_transfomation_map {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        DataStreamSource<String> words = env.socketTextStream("linux01",8888);
        //1.数据变大写
        SingleOutputStreamOperator<String> upper = words.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        });
        //map方法是并行的算子,默认和运行环境保持一致
/*        SingleOutputStreamOperator<Tuple2<string,Integer>> upper = words.map((s,t)-> {
            String upp = s.toUpperCase(Locale.ROOT);
        });*/
        upper.print();

        env.execute();




    }

}
