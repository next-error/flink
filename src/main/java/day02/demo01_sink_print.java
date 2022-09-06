package day02;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.Locale;


public class demo01_sink_print {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(4);
        DataStreamSource<String> lines = env.socketTextStream("linux01",8888);
        SingleOutputStreamOperator<String> maped = lines.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase(Locale.ROOT);
            }
        });
        maped.addSink(new MyprientSink());
        //lines.print();
        env.execute();

    }
    public static class MyprientSink extends RichSinkFunction<String> {
        @Override
        public void invoke(String value, Context context) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println((indexOfThisSubtask + 1) + ">" + value);
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close method is invoked");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open method is invoked");
        }
    }
}
