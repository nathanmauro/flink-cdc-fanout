//package io.nathanmauro.flink.streaming.etl;
//
//import java.util.Objects;
//import java.util.concurrent.TimeUnit;
//
//import io.nathanmauro.flink.streaming.etl.data.StriimEventType;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
//import org.apache.flink.streaming.api.functions.sink.SinkFunction;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//
//public class StreamingJob {
//
//    private final SourceFunction<String> source;
//    private final SinkFunction<Tuple3<Long, String, Integer>> sink;
//
//    public StreamingJob(
//            SourceFunction<String> source, SinkFunction<Tuple3<Long, String, Integer>> sink) {
//        this.source = source;
//        this.sink = sink;
//    }
//
//    public static void main(String[] args) throws Exception {
//        StreamingJob job = new StreamingJob(new IntegerListSource(), new PrintSinkFunction<>());
//        job.execute();
//    }
//
//    public void execute() throws Exception {
//        ObjectMapper objectMapper = new ObjectMapper();
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        TypeInformation.of(String.class).createSerializer(env.getConfig());
//
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStream<StriimEventType> event = env.addSource(source).map(ev -> {
//            try {
//                StriimEventType st = objectMapper.readValue(ev, StriimEventType.class);
//                System.out.println("ChangeEvent property tableName: " + st.getMetadata().getTableName());
//                System.out.println("ChangeEvent property typeUUID: " + st.getMetadata().getOperationType());
//                System.out.println("ChangeEvent property txnID: " + st.getMetadata().getTxnID());
//
//                return objectMapper.readValue(ev, StriimEventType.class);
//            }
//            catch (Exception e) {
//                System.out.println("Exception in parsing the input records to Event POJO. "
//                        + "Please make sure the input record structure is compatible with the POJO. Input record: "
//                        + ev);
//                return null;
//            }
//        })
//                .returns(TypeInformation.of(new TypeHint<StriimEventType>() {
//                }))
//                .assignTimestampsAndWatermarks(
//                        new BoundedOutOfOrdernessWatermarkAssigner<>(Time.of(100, TimeUnit.MILLISECONDS)));
//        event
//                .filter(Objects::nonNull)
//                .keyBy(striimEventType -> {
//                    return striimEventType.getMetadata().getTableName();
//                })
////                .process()
//                .process(new EventTimeWindowCounter(Time.of(1, TimeUnit.SECONDS)))
//                .addSink(sink);
//
//        env.execute();
//    }
//}
