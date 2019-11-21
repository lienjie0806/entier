package com.entier.flink;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

public class FlinkToHbase {
    private static String zkServer = "134.96.33.134,134.96.179.35,134.96.179.37";
    private static String port = "11001";
    private static TableName tableName = TableName.valueOf("flink_hbase");
    private static final String cf = "cf1";
    private static final String topic = "flink_topic";

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), configOfKafka()));
        source.rebalance().map(new MapFunction<String, Object>() {
            private static final long serialVersionUID = 1L;
            public Object map(String value) throws Exception {
                System.out.println(value);
                write2HBase(value);
                System.out.println(value);
                return value;
            }
        }).print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Properties configOfKafka() {

        Properties props = new Properties();
        props.put("bootstrap.servers", "134.96.179.37:9092");
        props.put("group.id", "flinkToHbase");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public static void write2HBase(String value) throws IOException {
        Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", zkServer);
        config.set("hbase.zookeeper.property.clientPort", port);

        Connection connect = ConnectionFactory.createConnection(config);
        Admin admin = connect.getAdmin();
        if (!admin.tableExists(tableName)) {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            hTableDescriptor.addFamily(new HColumnDescriptor(cf));
            admin.createTable(hTableDescriptor);
        }

        Table table = connect.getTable(tableName);
        TimeStamp ts = new TimeStamp(new Date());
        Date date = ts.getDate();
        Put put = new Put(Bytes.toBytes(date.getTime()));
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes("test"), Bytes.toBytes(value));
        table.put(put);
        table.close();
        connect.close();
    }
}
