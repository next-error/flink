package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 先KeyBy,再按照ProcessingTime划分窗口
 * 当窗口触发后,每分区的每个组,都会触发输出
 *调用Reduce,增量聚合
 */
public class demo07_KeyedTumblingWindow_Apply {
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
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.window(TumblingProcessingTimeWindows.of(Time.seconds(20)));
        //SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = windowedStream.sum(1);
        SingleOutputStreamOperator<List<Tuple2<String, Integer>>> res = windowedStream.apply(new WindowFunction<Tuple2<String, Integer>, List<Tuple2<String, Integer>>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<List<Tuple2<String, Integer>>> out) throws Exception {
                out.collect((List<Tuple2<String, Integer>>) input);
            }
        });
        res.print();
        env.execute();



    }
}
