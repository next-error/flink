package day05;

import Utils.Flink_Local_Connection;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * 不使用Flink的状态编程API,而是自己
 */
public class demo06_MyState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = Flink_Local_Connection.getConnection();
        env.setParallelism(3);
        env.enableCheckpointing(5000);
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    if (word.startsWith("error")) {
                        throw new RuntimeException();
                    }
                    collector.collect(word);
                }
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2 map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyed = maped.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });
        //KeyBy后,一个分区可以有多个key
        //SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.sum("f1");
        //自己实现类似sum的功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = keyed.map(new MySumFunction());

        sumed.print();

        env.execute();

    }

    /**
     * 自己定义实现sum功能的Function
     * 实现正确的累加(相同分区不同的key)
     * 实现容错
     */
    public static class MySumFunction implements MapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>{
        //private int count;
        private HashMap<String,Integer> counter = new HashMap<>();
        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
            String word = input.f0;
        //根据单词到map中取数据
            Integer count =counter.get(word);
            if (count == null) {
                count = 0;
            }
            count += input.f1;
            //更新map
            counter.put(word,count);
            return Tuple2.of(word,count);
        }
    }

    /**
     * 上述实现了1,但没能容错
     */
    public static class MySumFunction2 implements MapFunction<Tuple2<String,Integer>,Tuple2<String,Integer>>{
        //private int count;
        private HashMap<String,Integer> counter = new HashMap<>();
        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> input) throws Exception {
            String word = input.f0;
            //根据单词到map中取数据
            Integer count =counter.get(word);
            if (count == null) {
                count = 0;
            }
            count += input.f1;
            //更新map
            counter.put(word,count);
            return Tuple2.of(word,count);
        }
    }
}
