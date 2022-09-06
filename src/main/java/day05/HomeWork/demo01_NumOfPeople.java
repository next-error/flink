package day05.HomeWork;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;

/**
 * a001,view,u001
 * a001,view,u001
 * a001,click,u001
 * a002,view,u003
 * 统计每个广告不同事件的人数,需要去重
 */

public class demo01_NumOfPeople {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        env.setParallelism(2);
        env.enableCheckpointing(3000);
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple2<String,  HashSet<String> >> messWithOne = lines.map(new MapFunction<String, Tuple2<String,  HashSet<String>>>() {
            @Override
            public Tuple2<String,  HashSet<String>> map(String line) throws Exception {
                String[] fields = line.split(",");
                String key = fields[0] + "-" + fields[1];
                HashSet<String> uid = new HashSet<>();
                uid.add(fields[2]);
                return Tuple2.of(key, uid);
            }
        });
        KeyedStream<Tuple2<String, HashSet<String>>, String> keyed = messWithOne.keyBy(t -> t.f0);

        //keyed.print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> res = keyed.process(new SumPerson());
        res.print();
        env.execute();
    }
    public static class SumPerson extends KeyedProcessFunction<String,Tuple2<String,HashSet<String>>,Tuple2<String,Integer>>{
    private ValueState<HashSet<String>> valueState;
        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<HashSet<String>> stateDescriptor = new ValueStateDescriptor<>("countPerson", TypeInformation.of(new TypeHint<HashSet<String>>() {
            }));
            valueState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void processElement(Tuple2<String,  HashSet<String>> input, Context ctx, Collector<Tuple2<String,  Integer>> out) throws Exception {
            Integer count ;
            HashSet<String> value = valueState.value();
            if (value==null) {
                count =0;
                //value = new HashSet<>();
            }
            value.add(input.f1.toString());
            valueState.update(value);
            count = Integer.parseInt(String.valueOf(value.size()));
           // System.out.println(value.toString());
            out.collect(Tuple2.of(input.f0,count));


        }
    }
}
