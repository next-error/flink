package day10;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

public class demo02_MysqlAsyncFunction extends RichAsyncFunction<String, Tuple2<String,String>>{
    @Override
    public void asyncInvoke(String input, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {


    }
}
