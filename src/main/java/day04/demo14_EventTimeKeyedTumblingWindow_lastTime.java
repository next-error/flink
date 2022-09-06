package day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 按照EventTime划分滚动窗口（没有keyBy）
 * 并且执行允许乱序延迟时间 > 0
 *
 * 如果生成WaterMark的DataStream并行度为1
 *
 *   WaterMark = 该分区中最大的EventTime - 延迟时间
 *
 *   窗口的触发时机 = WaterMark >= 窗口的结束时间（闭区间）
 *
 */
public class demo14_EventTimeKeyedTumblingWindow_lastTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 9999);
        //env.getConfig().setAutoWatermarkInterval(200); 设置WartMark周期时间
        //Watermark = 每个分区中最大的EventTime - 延迟时间
        //窗口触发时机 WaterMark >= 窗口的结束边界
        //1000,spark,1

        //提取数据中的时间,生成wartMark(特殊信号,触发EventTime类型窗口的统一标准)
        //返回的不但有对应的数据,还有特殊的信号(WaterMark)
        //调用完该方法,不会改变原来数据,仅仅多了WaterMark
        SingleOutputStreamOperator<String> linesWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) { //数据延迟触发的时间 2秒
            @Override
            public long extractTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = linesWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, Integer> keyed = tpStream.keyBy(t -> t.f1);
        //划分滚动窗口,不KeyB
/*        AllWindowedStream<Integer, TimeWindow> res = keyed.window(TumblingEventTimeWindows.of(Time.seconds(10)));
        SingleOutputStreamOperator<Integer> sumed = res.sum(0);
        sumed.print();*/
        env.execute();
    }
}
