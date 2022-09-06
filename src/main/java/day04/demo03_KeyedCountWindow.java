package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 *
 * 先KeyBY,再划分CountWindow,
 *
 * KeyedWindow,对应的并行度,可以多个
 *
 * KeyBy后划分窗口,当一个分区的一个组的数据达到指定条数触发
 *
 */
public class demo03_KeyedCountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = map.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> keyedStream = keyed.countWindow(5);
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyedStream.sum(1);
        //resStream.print();
        sumed.print();
        env.execute();



    }
}
