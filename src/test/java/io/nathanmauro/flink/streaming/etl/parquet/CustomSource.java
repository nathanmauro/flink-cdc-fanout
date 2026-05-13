package io.nathanmauro.flink.streaming.etl.parquet;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomSource {

    private static final Date expiration = new Date(System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60));

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        streamEnv.setParallelism(1);
        streamEnv.enableCheckpointing(3000, CheckpointingMode.EXACTLY_ONCE);

        StreamingFileSink<TextOut> sink = StreamingFileSink.forBulkFormat(
                new Path("file:///c:/tmp/test2"),
                ParquetAvroWriters.forReflectRecord(TextOut.class)
        ).build();

        DataStreamSource<TextOut> customSource = streamEnv.addSource(fakeSourceTxt(), TypeInformation.of(TextOut.class));

        customSource.print();
        customSource.addSink(sink);

        streamEnv.execute();
    }

    private static SourceFunction<TextOut> fakeSourceTxt() {
        return new SourceFunction<TextOut>() {
            @Override
            public void run(SourceContext<TextOut> ctx) throws Exception {
                List<String> lines = Arrays.asList("how are you", "you are how", " i am fine");

                long date = new Date(System.currentTimeMillis()).toInstant().getEpochSecond();
                long expirationEpoch = expiration.toInstant().getEpochSecond();
                while (date < expirationEpoch) {
                    int index = new Random().nextInt(3);
                    ctx.collect(new TextOut(lines.get(index)));
                    Thread.sleep(200);
                }
            }

            @Override
            public void cancel() {
            }
        };
    }

    private static SourceFunction<String> fakeSourceJson() {
        return new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                int count = 0;
                while (count < 1) {
                    String deserializedString = "";
                    String inputJSON = "";
                    String jsonPath = new File("./").getCanonicalPath();
                    if (count == 0) {
                        jsonPath += "/src/main/resources/striim_eap_dev.json";
//                        byte[] bytes = Files.readAllBytes(Paths.get(jsonPath));
//                        inputJSON = new String(bytes);
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
}
