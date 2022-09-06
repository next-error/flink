package day12;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class demo01_WordCount_sql_newAPI {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> maped = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });
        Schema schema = Schema.newBuilder()
               .column("word", DataTypes.STRING())
                .column("count",DataTypes.INT())
                .build();
        tenv.createTemporaryView("v_wc", maped,schema );

        //写SQL
       // TableResult tableResult = tenv.executeSql("select word, sum(counts) total_counts from v_wc group by word");
        TableResult tableResult = tenv.executeSql("desc v_wc");

        tableResult.print();
        env.execute();


    }
}
