package day01;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.Arrays;
import java.util.List;

public class demo02_source4 {
    /**
     * 演示flink流计算额SocketSource,指定以后生成的Task,从指定的地址和端口号读取
     * 基于集合并行的的source,有限数据流,程序读取完数据,程序退出
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //只能本地环境运行,不能集群中运行,并且有WebUI
        Configuration conf = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        //Source分为并行的和非并行的
        //env.fromParallelCollection(new LongValueSequenceIterator((1,10));

        env.execute();


    }
}
