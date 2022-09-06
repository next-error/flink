package day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Flink有两种状态,KeyedState 和 OperatorState 只有KeyedState有TTL
 *
 *  ValueState MapState  ListState 有TTL (Time To LIve)
 *
 *  先使用ValueState
 *      CowMap<Key, Inter> 设置TTL 是对K-V设置TTL
 *  MapState
 *      CowMap<Key, Map<k-v>> 设置TTL 是对k-v设置TTL
 *  ListState
 *      CowMap<Key, List<E>> 设置TTL,为List中每个元素设置TTL
 *
 */
public class demo03_ValueState_TTL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //
        //读取数据
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        /**
         * u001,search
         * u001,view
         * u001,addCart
         * u001,pay
         * u002,view
         * u002,view
         * u002,addCart
         *
         */
        //对数据进行整理
        SingleOutputStreamOperator<Tuple2<String, String>> tpStream = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        //按照用户ID进行KeyBy
        KeyedStream<Tuple2<String, String>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<List<String>> res = keyedStream.process(new MyEventListStateFunction());

        res.print();

        env.execute();


    }

    public static class MyEventListStateFunction extends KeyedProcessFunction<String, Tuple2<String, String>, List<String>> {

        private transient ListState<String> listState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.seconds(5)).build();
            ListStateDescriptor<String> stateDescriptor = new ListStateDescriptor<String>("lst-state", String.class);
            stateDescriptor.enableTimeToLive(ttlConfig);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> value, Context ctx, Collector<List<String>> out) throws Exception {

            String event = value.f1;
            listState.add(event);
            //Iterable<String> iterable = new Iterable<>();
            //ArrayList<String> events = (ArrayList<String>) listState.get();
            Iterable<String> iterable = listState.get();
            ArrayList<String> events = new ArrayList<>();
            for (String s : iterable) {
                events.add(s);
            }

            out.collect(events);

        }
    }
}
