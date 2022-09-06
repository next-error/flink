package day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class demo06_Shuffle {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);
        SingleOutputStreamOperator<String> upper = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                int indexOfThisSubtask = getIterationRuntimeContext().getIndexOfThisSubtask();
                return indexOfThisSubtask + ">" + s;
            }
        }).setParallelism(2);
        DataStream<String> shuffled = upper.shuffle();
        shuffled.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int indexOfThisSubtask = getIterationRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + ">" + indexOfThisSubtask);
            }
        }).setParallelism(4);
        env.execute();
    }
}
