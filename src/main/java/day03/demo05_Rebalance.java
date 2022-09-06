package day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.concurrent.TransferQueue;

/**
 * 上下游并行度不一致时,默认rebalnce
 */
public class demo05_Rebalance {
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
        DataStream<String> rebalanced = upper.rebalance();
        rebalanced.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value) throws Exception {
                int indexOfThisSubtask = getIterationRuntimeContext().getIndexOfThisSubtask();
                System.out.println(indexOfThisSubtask + ">" + value);
            }
        }).setParallelism(2);
        env.execute();
    }
}
