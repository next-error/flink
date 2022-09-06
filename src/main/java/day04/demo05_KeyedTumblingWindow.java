package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先KeyBy,再按照ProcessingTime划分窗口
 * 当窗口触发后,每分区的每个组,都会触发输出
 *
 * 如果调用sum,reduce,min,max方法, 在窗口内增量聚合,,触发后将增量聚合的结果输出
 */
public class demo05_KeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = map.keyBy(t -> t.f0);
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = windowedStream.sum(1);
        sumed.print();
        env.execute();



    }
}
