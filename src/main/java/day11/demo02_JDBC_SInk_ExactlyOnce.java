/*
package day11;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

*/
/**
 * jdbc sink 的使用,将flink处理后的数据写入MySQL
 *//*

public class demo02_JDBC_SInk_ExactlyOnce {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("linux01", 8888);

        //将处理后的数据写入MySQL

        //1,tom,18

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> maped = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                long id = Long.parseLong(fields[0]);
                String name = fields[1];
                int age = Integer.parseInt(fields[2]);
                return Tuple3.of(id, name, age);
            }
        });

        //使用JDBC Sink (第一种,AtleastOnce)
        String sql ="insert into ";

        SinkFunction<Tuple3<Long, String, Integer>> mysqlSink = JdbcSink.sink(
                sql,
                //将参数和prepareStatement进行映射
                new JdbcStatementBuilder<Tuple3<Long, String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Tuple3<Long, String, Integer> tp) throws SQLException {
                        preparedStatement.setLong(1, tp.f0);
                        preparedStatement.setString(2, tp.f1);
                        preparedStatement.setInt(3, tp.f2);
                    }
                },
                //设置执行相关参数
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(5)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl()
                        .withDriverName()
                        .withUsername()
                        .withPassword()
                        .build()
        );



    }


}
*/
