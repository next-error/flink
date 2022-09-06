package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 按照EventTime划分滚动窗口（没有keyBy）
 */
public class demo12_EventTimeNoneKeyedTumblingWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //env.getConfig().setAutoWatermarkInterval(200); 设置WartMark周期时间
        //Watermark = 每个分区中最大的EventTime - 延迟时间
        //窗口触发时机 WaterMark >= 窗口的结束边界
        //1655970841,1  输入该数据,就确认了时间范围,生成了一个窗口
        //1655970535,2
        //1655971290,3
        //1655971390,4
        //1655971490,5

        //提取数据中的时间,生成wartMark(特殊信号,触发EventTime类型窗口的统一标准)
        //返回的不但有对应的数据,还有特殊的信号(WaterMark)
        //调用完该方法,不会改变原来数据,仅仅多了WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) { //数据延迟触发的时间
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Integer> nums = linesWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                String[] fields = s.split(",");
                return Integer.parseInt(fields[1]);
            }
        });

        //划分滚动窗口,不KeyB
        //设置窗口长度
        AllWindowedStream<Integer, TimeWindow> res = nums.windowAll(TumblingEventTimeWindows.of(Time.seconds(10)));//
        SingleOutputStreamOperator<Integer> sumed = res.sum(0);
        sumed.print();
        env.execute();
    }
}
