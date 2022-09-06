package day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy的会话窗口（ProcessingTime类型）
 *
 * 当前系统时间 - 上一条进入到窗户中的数据对应的使用 > 字段的时间间隔，窗口触发
 *
 * NonKeyedWindow，Window和WindowOperator所在的DataStream并行度为1
 *
 * ProcessingTimeSessionWindows即ProcessingTIme类型的回话窗口
 *
 */
public class demo10_NoneKeyedSessionWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //,没有keyBy直接划分窗口:ProcessingTimeSessionWindows
        AllWindowedStream<Integer, TimeWindow> windowedStream = nums.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> sumed = windowedStream.sum(0);
        sumed.print();
        env.execute();



    }
}
