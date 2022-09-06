package day06;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import javax.naming.MalformedLinkException;
import java.util.ArrayList;
import java.util.List;

    /**
     * KeyState三种类型
     *  ValueState<Integer>  Map<Key, Integer>
     *  MapState<String,Integer>  Map<Key, Map<String,Integer>>
     *  ListState<Integer>  Map<Key, List<Integer>>
     *
     * 演示LIstState
     *  同一个用户,在浏览购物网站,将用户最近的10个行为保存
     *  用户 view
     *  u001,view
     *  u001,addCart
     *  u001,pay
     *  u002,view
     *
     */

@SuppressWarnings("all")
public class demo02_LIstState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple2<String, String>> streamOperator = lines.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String s) throws Exception {
                String[] fields = s.split(",");
                return Tuple2.of(fields[0], fields[1]);

            }
        });
        KeyedStream<Tuple2<String, String>, String> keyedStream = streamOperator.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, List<String>>> res = keyedStream.process(new MyEventFunction());
        res.print();

        env.execute();

    }

    public static class MyEventFunction extends KeyedProcessFunction<String,Tuple2<String,String>,Tuple2<String, List<String>>>{
private transient ListState<String> listState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<String> listStateDescriptor = new ListStateDescriptor<>("list-state", String.class);
           listState = getRuntimeContext().getListState(listStateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String, String> input, KeyedProcessFunction<String, Tuple2<String, String>, Tuple2<String, List<String>>>.Context ctx, Collector<Tuple2<String, List<String>>> out) throws Exception {
            String uid = input.f0;
            String event = input.f1;
            listState.add(event);
            Iterable<String> iterable = listState.get();
            ArrayList<String> events = new ArrayList<>();

            for (String s : iterable) {
                events.add(s);
            }

            out.collect(Tuple2.of(uid,events));


        }
    }
}
