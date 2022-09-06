
package day08;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.util.stream.Stream;


/**
 * 将当前窗口的数据进行增量聚合,让再将聚合后的数据于历史状态进行聚合
 *
 * 如果一条一条聚合,实时性高,但每次来一条数据输出一条数据,输出到外部存储系统压力较大
 *
 * 用reduce聚合,输入和输出类型必须要保持一致,比较死板
 * 采用aggregate聚合,这是更底层的方法,用起来更加灵活
 */

public class demo02_WindowReduceWithHistoryState {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = map.keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //windowedStream.aggregate();
    }
    public static class MyAggregateFunction implements AggregateFunction<Tuple2<String,Integer>,Integer,Integer> {

        // 设置每个窗口,每个key的初始值
        @Override
        public Integer createAccumulator() {
            return 0;
        }
        //每个key输入一次,调用一次add方法
        @Override
        public Integer add(Tuple2<String, Integer> input, Integer accumulator) {
            return accumulator += input.f1;
        }
        //窗口触发后,返回的结果
        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }
        //只有会话窗口可能调用该方法
        @Override
        public Integer merge(Integer integer, Integer acc1) {
            return null;
        }
    }



    //窗口处理完毕输出结果后,每个key会调用一次apply,和之前状态保存的中间结果再聚合
    public static class MyProcessFUnction extends ProcessWindowFunction<Integer,Tuple2<String,Integer>,String,TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Integer, Tuple2<String, Integer>, String, TimeWindow>.Context context, java.lang.Iterable<Integer> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

        }
    }
}

