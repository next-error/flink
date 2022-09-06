package day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 使用侧流输出, 获取窗口的迟到数据
 *
 * 本质上,将迟到的数据打上标签,根据标签获取迟到数据
 */
public class demo07_EventTimeTumblingWindow_GetLateData {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //WatermarkStrategy<String> linesWithWaterMark = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {

            @Override
            public long extractTimestamp(String s, long l) {
                String[] fields = s.split(",");
                return Long.parseLong(fields[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, Integer> keyed = tpStream.keyBy(t -> t.f1);
        //划分滚动窗口,不KeyB
       //AllWindowedStream<Integer, TimeWindow> res = keyed.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        //SingleOutputStreamOperator<Integer> sumed = res.sum(0);
        //sumed.print();
        env.execute();
    }
}
