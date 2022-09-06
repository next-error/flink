package day06;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class MySumByProvince extends KeyedProcessFunction<String, Tuple3<String, String,Integer>,Tuple2<String,Integer>> {
    private transient MapState<String, Integer> mapState;
    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Integer> citySUmState = new MapStateDescriptor<>("provinceSUmState", String.class, Integer.class);
        mapState = getRuntimeContext().getMapState(citySUmState);
    }

    @Override
    public void processElement(Tuple3<String, String, Integer> input, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
        String province = input.f0;
        String city = input.f1;
        Integer moneyByCities = input.f2;
        mapState.put(city,moneyByCities);
        Iterable<Integer> iterable = mapState.values();
        int moneyByProvinces = 0;
        for (Integer money : iterable) {
            moneyByProvinces += money;
        }
        //
        out.collect(Tuple2.of(province,moneyByProvinces));

    }
}
