//package io.nathanmauro.flink.streaming.etl;
//
//import org.apache.flink.api.common.serialization.SimpleStringEncoder;
//import org.apache.flink.api.java.utils.ParameterTool;
//import org.apache.flink.core.fs.Path;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
//
//public class KafkaToHDFSAvroJob {
//    private static Logger LOG = LoggerFactory.getLogger(KafkaToHDFSAvroJob.class);
//    public static void main(String[] args) throws Exception {
//        ParameterTool params = Utils.parseArgs(args);
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        KafkaDeserializationSchema<Message> schema = ClouderaRegistryKafkaDeserializationSchema
//                .builder(Message.class)
//                .setConfig(Utils.readSchemaRegistryProperties(params))
//                .build();
//        FlinkKafkaConsumer<Message> consumer = new FlinkKafkaConsumer<Message>(params.getRequired(K_KAFKA_TOPIC), schema, Utils.readKafkaProperties(params));
//
//        DataStream<String> source = env.addSource(consumer)
//                .name("Kafka Source")
//                .uid("Kafka Source")
//                .map(record -> record.getId() + "," + record.getName() + "," + record.getDescription())
//                .name("ToOutputString");
//        StreamingFileSink<String> sink = StreamingFileSink
//                .forRowFormat(new Path(params.getRequired(K_HDFS_OUTPUT)), new SimpleStringEncoder<String>("UTF-8"))
//                .build();
//        source.addSink(sink)
//                .name("FS Sink")
//                .uid("FS Sink");
//        source.print();
//
//        env.execute("Flink Streaming Secured Job Sample");
//    }
//}
