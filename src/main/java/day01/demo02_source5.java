package day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class demo02_source5 {
    /**
     * 演示flink流计算额SocketSource,指定以后生成的Task,从指定的地址和端口号读取
     * 基于文件的的source,即生成的source生成的task可以在文件中读取数据
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //只能本地环境运行,不能集群中运行,并且有WebUI
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //Source分为并行的和非并行的
        DataStreamSource<String> rtf = env.readTextFile("d:\\g\\aaa\\a.txt");

        env.execute();


    }
}
