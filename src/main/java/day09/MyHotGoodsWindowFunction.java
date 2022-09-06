package day09;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class MyHotGoodsWindowFunction implements WindowFunction<Integer, MyHotGoodsItem, Tuple3<String, String, String>, TimeWindow> {
    @Override
    public void apply(Tuple3<String, String, String> key, TimeWindow window, Iterable<Integer> input, Collector<MyHotGoodsItem> out) throws Exception {
        long start = window.getStart();
        long end = window.getEnd();
        Integer count = input.iterator().next();
        String event = key.f0;
        String productID = key.f1;
        String categoryID = key.f2;
        out.collect(new MyHotGoodsItem(categoryID,productID,event,count,start,end));
    }
}
