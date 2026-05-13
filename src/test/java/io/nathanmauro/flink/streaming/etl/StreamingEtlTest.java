//package io.nathanmauro.flink.streaming.etl;
//
//import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
//import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.test.util.MiniClusterWithClientResource;
//import org.junit.ClassRule;
//import org.junit.Test;
//
//import static org.assertj.core.api.Assertions.assertThat;
//import static org.assertj.core.api.Assertions.not;
//
//public class StreamingEtlTest {
//
//    @ClassRule
//    public static MiniClusterWithClientResource flinkCluster =
//            new MiniClusterWithClientResource(
//                    new MiniClusterResourceConfiguration.Builder()
//                            .setNumberSlotsPerTaskManager(2)
//                            .setNumberTaskManagers(1)
//                            .build());
//
//    @Test
//    public void testCompletePipeline() throws Exception {
//
//        // Arrange
//        CollectingSink sink = new CollectingSink();
//
//        CollectionSource source = new CollectionSource();
//        StreamingJob job = new StreamingJob(source, sink);
//
//        // Act
//        job.execute();
//        System.out.println("test");
//
//        // Assert
//        // Long.MAX_VALUE watermark is sent at the end of finite source
//        assertThat(sink.values).doesNotContain("test");
////                .containsExactlyInAnyOrder(
////                        new Tuple3<>(1000L, 1, 1),
////                        new Tuple3<>(2000L, 1, 2),
////                        new Tuple3<>(2000L, 2, 2),
////                        new Tuple3<>(2000L, 3, 2),
////                        new Tuple3<>(3000L, 1, 3),
////                        new Tuple3<>(3000L, 2, 2),
////                        new Tuple3<>(3000L, 3, 2));
//    }
//
//    public static class CollectionSource implements SourceFunction<String> {
//        private volatile boolean cancelled = false;
//
//        @Override
//        public void run(SourceContext<String> ctx) throws Exception {
//            while (!cancelled) {
//                synchronized (ctx.getCheckpointLock()) {
//                    ctx.collectWithTimestamp(App.getRecord(), System.currentTimeMillis());
//                }
//            }
//        }
//
//        @Override
//        public void cancel() {
//            cancelled = true;
//        }
//    }
//}
