/*
package day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

*/
/**
 * ProcessionFunction 是Flink更加底层的方法,可以访问Flink程序更底层的属性和方法
 *
 *  优点:更灵活
 *  缺点:使用复杂一点
 *
 *  三种功能:
 *  1.对数据一条一条处理
 *  2.对KeyedStream使用KeyedState
 *  3.对KeyState使用定时器 (类似窗口功能)
 *
 *  定时器 + 状态
 *//*

public class demo05_ProcessFunctionTimer3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<String> andWatermarks = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String line) {
                String[] fields = line.split(",");
                return Long.parseLong(fields[0]);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> lineWithWaterMarker = andWatermarks.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = lineWithWaterMarker.keyBy(t -> t.f0);

        //使用processFunction + ListState , 计算 一端时间内相同单词次数的最大TopN
        keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String ,Integer>>() {
            //初始化状态
            public  ListState<Tuple2<String, Integer>> listState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Tuple2<String, Integer>> stateDescriptor = new ListStateDescriptor<Tuple2<String, Integer>>("list-state", TypeInformation.of());
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> input, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                long currentWatermark = ctx.timerService().currentWatermark();
                long triggerTime = currentWatermark - currentWatermark % 30000 + 30000;
                //注册EventTime类型定时器
                ctx.timerService().registerEventTimeTimer(triggerTime);
                //对30内数据处理l
                listState.add(input);


            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });


        env.execute();


    }

}
*/
