package org.example;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Main {
    static final String bootsrapServer = "localhost:9094";

    static final String schemaRegistry = "http://localhost:8081";

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // Kafka Source 테이블 정의
        tableEnv.executeSql(
                "CREATE TABLE kafka_source_table (" +
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
        tableEnv.executeSql(
                "SELECT * FROM kafka_source_table"
        ).print();
    }
}