package day09;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MyHotGoodsAggregateFunction implements AggregateFunction<Tuple3<String,String,String>,Integer,Integer> {

    @Override
    public Integer createAccumulator() {
        return 0;
    }

    @Override
    public Integer add(Tuple3<String, String, String> input, Integer acc) {
        return acc + 1;
    }

    @Override
    public Integer getResult(Integer acc) {
        return acc;
    }

    @Override
    public Integer merge(Integer integer, Integer acc1) {
        return null;
    }
}
