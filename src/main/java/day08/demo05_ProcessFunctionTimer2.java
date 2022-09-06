package day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

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
 */
public class demo05_ProcessFunctionTimer2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //spark,1

        SingleOutputStreamOperator<Tuple2<String, Integer>> map = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = map.keyBy(t -> t.f0);

        //使用定时器
        keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
            public ValueState<Integer> valueState;
            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<Integer>("history-state", Integer.class);
                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                //注册定时器
                long currentTime = ctx.timerService().currentProcessingTime();
                long trigger_Time = currentTime - currentTime % 30000 + 30000;
                //同一个key,注册了多个定时器,仅仅会触发一次,后面的key会覆盖之前的
                ctx.timerService().registerEventTimeTimer(trigger_Time);


            }
            //当ProcessingTime >= 注册器时间 onTime方法调用
            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>.OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

            }
        });
        //process.print();
        env.execute();


    }

}
