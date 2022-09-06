package day04.slidling;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * 没有KeyBy的滑动窗口（ProcessingTime类型）
 *
 * NonKeyedWindow，Window和WindowOperator所在的DataStream并行度为1
 *
 * SlidingProcessingTimeWindows即ProcessingTIme类型的滚动窗口，会按照系统时间，生成窗口，即使没有数据输入，也会形成窗口
 *
 */
public class demo08_NoneKeyedSlidingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //,没有keyBy直接划分窗口
        // 指定划分窗口的的方式，即按照ProcessingTime划分滑动窗口，需要传入两个时间，第一个是窗口的长度，第二个是滑动的步长
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)));
        SingleOutputStreamOperator<Integer> sumed = windowedStream.sum(0);
        sumed.print();
        env.execute();



    }
}
