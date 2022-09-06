package day09;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 * 数据流为什么要join关联 : 想要的数据,来自两个或多个流, 而且流之间存在着关联关系
 *
 *  实时的数据如何关联到一起 (数据一闪而过,无法关联)  :
 *  1.相同的时间地点--按照相同的条件分区
 *  2.两个流的数据必须在相同的时间范围内: (划分窗口(WindowJoin)  connect后使用状态(IntervalJoin))
 *
 *  演示EventTime滚动窗口InnerJoin
 */
/*public class demo02_EventTimeTumblingJoin {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,o001,已支付 时间,订单号,状态
        DataStreamSource<String> lines1 = env.socketTextStream("linux01", 8888);
        //1001,o001,3000,手机  时间,订单号,金额
        //1001,o001,3000,家具  时间,订单号,金额
        DataStreamSource<String> lines2 = env.socketTextStream("linux01", 9999);

        //两个流join
        //o001,已支付,家具,2000
        //o001,已支付,手机,3000
        SingleOutputStreamOperator<String> linesWithWaterMaker1 = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return Long.parseLong(s.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<String> linesWithWaterMaker2 = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String s, long l) {
                return Long.parseLong(s.split(",")[0]);
            }
        }));

        SingleOutputStreamOperator<Tuple2<String, String>> leftStream = linesWithWaterMaker1.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[1], fields[2]);
            }
        });
        SingleOutputStreamOperator<Tuple3<String, String, Double>> rightStream = linesWithWaterMaker2.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple3.of(fields[1], fields[2], Double.parseDouble(fields[3]));
            }
        });
        leftStream.join(rightStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple2<String, String>, Tuple3<String, String, Double>, Tuple4<String,String,String,Double>>() {
                    //窗口触发后调用
                    @Override
                    public Tuple4<String, String, String, Double> join(Tuple2<String, String> left, Tuple3<String, String, Double> right) throws Exception {

                        return Tuple4.of(left.f0,left.f1,right.f1,Double.parseDouble(right.f2));
                    }
                })
    }
}*/
