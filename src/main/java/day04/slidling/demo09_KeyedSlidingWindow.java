package day04.slidling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;





public class demo09_KeyedSlidingWindow {
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
                                                                                 // keyed.window(SlidingProcessingTimeWindows(Time.seconds(10), Time.seconds(20)));
        //window方法中传入WindowAssigner（划分窗口的方式）
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.window(SlidingProcessingTimeWindows.of(Time.seconds(20), Time.seconds(10)));
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);
        res.print();
        env.execute();



    }
}
