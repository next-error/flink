package day10;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

public class demo02_AsyncMYSQL {
    public static void main(String[] args) {
        int capacity =20;
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        AsyncDataStream.orderedWait(
                lines,
                new demo02_MysqlAsyncFunction(),
                3000,
                TimeUnit.MILLISECONDS,
                capacity);


    }
}
