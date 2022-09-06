package day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class demo01_word5 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        lines.flatMap(new myFlatMap()).keyBy(tp -> tp.f0).sum(1).print();

        env.execute();

    }
    public static class myFlatMap implements FlatMapFunction<String ,Tuple2<String,Integer>>{

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split(" ");
            for (String word : words)
                collector.collect(Tuple2.of(word, 1));
        }
    }
}
