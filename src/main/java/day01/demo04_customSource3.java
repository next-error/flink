package day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class demo04_customSource3 {
    /**
     * 自定义source,run方法非并行,run方法执行完,程序退出
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> line = env.addSource(new MyParallSource());
        line.print();
        env.execute();
    }
    public static class MyParallSource extends RichParallelSourceFunction<String>{
        private boolean flag =true;
        @Override
        public void open(Configuration parameters) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("@@@@@ open method invoked!!!!"+ indexOfThisSubtask);
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("@@@@@ run method invoked!!!!" + indexOfThisSubtask);
            while (flag){
                ctx.collect(UUID.randomUUID().toString());
                Thread.sleep(8000);
            }
        }

        @Override
        public void cancel() {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("@@@@@ cancel method invoked!!!!" +indexOfThisSubtask);
            flag=false;
        }

        @Override
        public void close() throws Exception {
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
            System.out.println("$$$$$$ close method invoked in subtask : " + indexOfThisSubtask);
        }
    }
}
