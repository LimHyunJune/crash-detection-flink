package org.example;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.example.dto.Accelerometer;

public class Main {
    static final String bootsrapServer = "localhost:9094";

    static final String schemaRegistry = "http://localhost:8081";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Kafka Source 테이블 정의
        tableEnv.executeSql(
                "CREATE TABLE accelerometer (" +
                        "  duid STRING," +
                        "  amount DOUBLE" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'accelerometer'," +
                        "  'properties.bootstrap.servers' = '"+ bootsrapServer+ "'," +
                        "  'properties.group.id' = 'flink-group'," +
                        "  'key.format' = 'raw'," +      // key를 String으로 처리
                        "  'key.fields' = 'duid'," +
                        "  'value.format' = 'avro-confluent'," +
                        "  'value.avro-confluent.schema-registry.url' = '"+ schemaRegistry + "'," +
                        "  'value.fields-include' = 'EXCEPT_KEY'," +
                        "  'scan.startup.mode' = 'earliest-offset'" +
                        ")"
        );

        // 데이터 처리 및 결과 출력
        Table accelerometer = tableEnv.from("accelerometer");
//        accelerometer.execute().print();
        DataStream<Row> dataStream = tableEnv.toDataStream(accelerometer);
        DataStream<String> resultStream = dataStream.map((MapFunction<Row, String>) row -> {
            // 각 필드의 값을 가져오기
            String duid = (String) row.getField(0);
            Double amount = (Double) row.getField(1);

            // 필드 값을 포함한 문자열 반환
            return "duid: " + duid + ", amount: " + amount;
        });

        // 결과 출력
        resultStream.print();
        // Flink 작업 실행
        env.execute("DataStream Row Field Example");
    }
}