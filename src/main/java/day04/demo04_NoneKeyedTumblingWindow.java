package day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy的滚动窗口,(processingStream类型)
 *
 * TumblingProcessingTimeWindows即ProcessingTime类型的滚动窗口,会按照系统时间生成窗口  即使没有数据输入,也会生成窗口
 */
public class demo04_NoneKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //,没有keyBy直接划分窗口
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> sumed = windowedStream.sum(0);
        sumed.print();
        env.execute();



    }
}
