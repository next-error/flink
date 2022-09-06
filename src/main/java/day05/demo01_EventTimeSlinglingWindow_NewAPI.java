//package day05;
//
//import Utils.Flink_Local_Connection;
//import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//
//import java.time.Duration;
//
///**
// * 滑动窗口的时间范围
// * startTime =输入的时间 -输入的时间 % 滑动步长 + 滑动步长 -窗口长度   1100 - 1100 % 2000 + 2000-5000=-3000
// * endTime =输入的时间 -输入的时间 % 滑动步长 + 滑动步长  2000
// */
//public class demo01_EventTimeSlinglingWindow_NewAPI {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
//        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
//        //
//        WatermarkStrategy<String> andWatermarks = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
//            @Override
//            public long extractTimestamp(String s, long l) {
//                String[] fields = s.split(",");
//                return Long.parseLong(fields[0]);
//            }
//
//        });
//

//        SingleOutputStreamOperator<Tuple2<String, Integer>> lineWithWaterMarker = andWatermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                String[] fields = s.split(",");
//                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
//            }
//        });
//        KeyedStream<Tuple2<String, Integer>, String> keyed = lineWithWaterMarker.keyBy(t -> t.f0);
//
//        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = keyed.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2)));
//
//        SingleOutputStreamOperator<Tuple2<String, Integer>> res = windowedStream.sum(1);
//        res.print();
//        env.execute();
//    }
//
//}
