package day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;

public class demo04_customSource {
    /**
     * 自定义source,run方法非并行,run方法执行完,程序退出
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        DataStreamSource<String> line = env.addSource(new SourceFunction<String>() {
            @Override
            //启动时调用的方法,用于产生数据
            public void run(SourceContext<String> ctx) throws Exception {
                System.out.println("run method invoked !");
                List<String> list = Arrays.asList("hadoop");
                for (String word : list) {
                    ctx.collect(word);
                }
            }

            @Override
            //
            public void cancel() {
                System.out.println("cancel method invoked");
            }
        });
        line.print();
        env.execute();
    }
}
