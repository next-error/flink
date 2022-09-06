
package day08;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.Iterable;

import java.io.IOException;


/**
 * 将当前窗口的数据进行增量聚合,让再将聚合后的数据于历史状态进行聚合
 *
 * 如果一条一条聚合,实时性高,但每次来一条数据输出一条数据,输出到外部存储系统压力较大
 */

public class demo03_WindowReduceWithHistoryState {
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

        windowedStream.reduce(new MyReduceFunction(),new MYWindowFunction());
    }
    public static class MyReduceFunction implements ReduceFunction<Tuple2<String, Integer>> {

        @Override
        public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
            tp1.f1 = tp1.f1 + tp2.f1;
            return tp1;
        }
    }

    public static class MYWindowFunction extends AbstractRichFunction implements WindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>,String,TimeWindow> {
        private transient ValueState<Integer> historyState;
        private Integer historyCount;
        //初始化或者恢复状态
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("history-state", Integer.class);
            historyState = getRuntimeContext().getState(stateDescriptor);
        }

        /**
         * 窗口执行完,每一个Key会调用一次apply方法,和历史状态累加
         * @param s
         * @param window
         * @param input
         * @param out
         */
        @Override
        public void apply(String s, TimeWindow window, java.lang.Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> stringIntegerTuple2 = input.iterator().next();
            //获取历史状态数据
            Integer historyCount = historyState.value();
            if (historyCount ==null){
                historyCount =0;
            }
            historyCount += stringIntegerTuple2.f1;
            historyState.update(historyCount);
            stringIntegerTuple2.f1 = historyCount;
            //输出结果
            out.collect(stringIntegerTuple2);
        }
    }
}

