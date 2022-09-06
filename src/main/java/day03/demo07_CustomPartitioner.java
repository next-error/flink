package day03;

import day01.demo01_word5;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Collector;

/**
 * 自定义分区器
 */
public class demo07_CustomPartitioner {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        //lines.flatMap(new demo01_word5.myFlatMap()).keyBy(tp -> tp.f0).sum(1).print();
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new demo01_word5.myFlatMap());
        DataStream<Tuple2<String, Integer>> partitioned = wordAndOne.partitionCustom(new MyCustomPartitioner(),
                t -> t.f0);
        DataStreamSink<Tuple2<String, Integer>> sinked = partitioned.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                int indexOfThisSubtask = getIterationRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + ">" + indexOfThisSubtask);
            }
        });


        env.execute();

    }
    public static class MyCustomPartitioner implements Partitioner<String>{
        /**
         *
         * @param key: 输入的key
         * @param numPartitons:下游分区数量
         * @return
         */

        @Override
        public int partition(String key, int numPartitons) {
            int partition =0;
            if("spark".equals(key))
                partition=1;
            else if ("haoddp".equals(key))
                partition =2;
            else if ("flink".equals(key))
                partition=3;
            return partition;
        }
    }
    public static class myFlatMap implements FlatMapFunction<String , Tuple2<String,Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = line.split(" ");
            for (String word : words)
                collector.collect(Tuple2.of(word, 1));
        }
    }
}
