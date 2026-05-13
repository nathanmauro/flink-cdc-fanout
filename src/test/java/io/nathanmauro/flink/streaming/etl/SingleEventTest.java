package io.nathanmauro.flink.streaming.etl;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;
import org.junit.jupiter.api.Test;

import io.nathanmauro.flink.streaming.etl.events.DmsEventLoc;
import io.nathanmauro.flink.streaming.etl.utils.DmsEventLocSchema;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SingleEventTest
{

    private static final ObjectMapper mapper = new ObjectMapper();

    private void buildFlinkJobCore(StreamExecutionEnvironment env, SourceFunction<String> source,
                                   SinkFunction<DmsEventLoc> sink)
    {
        final OutputTag<String> errorOutputTag = new OutputTag<String>("error-output",
                TypeInformation.of(String.class))
        {
        };

        SingleOutputStreamOperator<DmsEventLoc> stream = env.addSource(source)
                .map(ev -> new DmsEventLocSchema().deserialize(ev.getBytes(StandardCharsets.UTF_8)));
        stream.addSink(sink);
        stream.print();

        stream.getSideOutput(errorOutputTag).print();
    }

    // region Test Flink Job
//    @Test
//    void testFlinkJob() throws Exception
//    {
//        final OutputTag<String> errorOutputTag = new OutputTag<String>("error-output",
//                TypeInformation.of(String.class))
//        {
//        };
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        KeyedProcessFunction processor = new CountWithTimeoutFunction();
//        SingleOutputStreamOperator<DmsEventLoc> streamOperator = this.getStriimEventSingleOutputStreamOperator(env, fakeSource());
//
//        harness.open();
//
//        harness.processElement(createEvent("/src/main/resources/striim_eap_dev.json"),
//                Instant.now().toEpochMilli());
//
//        List<StreamRecord<? extends DmsEventLoc>> records = harness.extractOutputStreamRecords();
//        assertThat(records.size()).isEqualTo(0);
//
//        ConcurrentLinkedQueue<StreamRecord<String>> errorOutput = harness.getSideOutput(errorOutputTag);
//
//        if (errorOutput != null)
//            assertThat(errorOutput.size()).isEqualTo(1);
//    }
    // endregion Test Flink Job

    static DmsEventLoc output = null;

    @Test
    void endToEndTest() throws Exception
    {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SinkFunction<DmsEventLoc> fakeSink = getFakeSink();

        buildFlinkJobCore(env, fakeSource(), fakeSink);
        env.execute();
        String jsonFromStream = new ObjectMapper().writeValueAsString(output);
        System.out.println("Flink Output: " + jsonFromStream);
        assertThat(output.toString().length()).isEqualTo(0);
    }

    private SinkFunction<DmsEventLoc> getFakeSink() {
        return new SinkFunction<DmsEventLoc>() {
            @Override
            public void invoke(DmsEventLoc value, Context context) throws Exception {
                output = value;
            }
        };
    }

    private static SourceFunction<String> fakeSource()
    {
        return new SourceFunction<>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int count = 0;
                while (count < 1) {
                    String deserializedString = "";
                    String jsonPath = new File("./").getCanonicalPath();
                    if (count == 0) {
                        jsonPath += "/src/main/resources/dms-kinesis-single.json";
                        deserializedString = new String(Files.readAllBytes(Paths.get(jsonPath)));
                    }

                    ctx.collect(deserializedString);
                    Thread.sleep(1000);
                    count++;
                }
            }

            @Override
            public void cancel() {
            }
        };
    }

    private String createEvent(String filePath) throws IOException
    {
        byte[] bytes = Files.readAllBytes(Paths.get(filePath));

        return mapper.readValue(bytes, String.class);
    }
}
