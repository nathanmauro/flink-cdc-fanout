package io.nathanmauro.flink.streaming.etl;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.apache.http.Header;
import org.apache.http.HttpException;
import org.apache.http.HttpHost;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HttpContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import io.nathanmauro.flink.streaming.etl.events.DmsEventLoc;
import io.nathanmauro.flink.streaming.etl.utils.DmsEventLocSchema;
import com.fasterxml.jackson.core.JsonProcessingException;

import org.elasticsearch.common.xcontent.XContentType;
import vc.inreach.aws.request.AWSSigner;
import vc.inreach.aws.request.AWSSigningRequestInterceptor;

@SuppressWarnings("DuplicatedCode")
public class ESApp {

    static final AWSCredentialsProvider defaultCredentials = new DefaultAWSCredentialsProviderChain();
    static final String esEndpoint = "PLACEHOLDER_ES_ENDPOINT";

    public static void main(String[] args) throws IOException {
        final List<HttpHost> httpHosts = List.of(HttpHost.create(esEndpoint));
        final ESRequestInterceptor requestInterceptor = new ESRequestInterceptor();

        RestHighLevelClient client = getClient(httpHosts, requestInterceptor);

        DmsEventLoc event = new DmsEventLocSchema().deserialize(App.getRecord().getBytes(StandardCharsets.UTF_8));
        IndexRequest request = createIndexRequest(event);
        ActionListener listener = new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                System.out.println(indexResponse.toString());
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println(e.getMessage());
            }
        };
        client.indexAsync(request, RequestOptions.DEFAULT, listener);
//        GetRequest request = new GetRequest("dev_test_1");
//        request.id("1");
//        GetResponse response = client.get(request);
//        System.out.println(response.toString());
        System.out.println("test");
        client.close();
    }

    static IndexRequest createIndexRequest(DmsEventLoc element) throws IOException {
        String indexName = element.getMetadata().get("table-name").toString();
        String id = getId(element);

        XContentBuilder json = null;
        try {
            json = DmsEventLocSchema.getXContentJson(element);
        }
        catch (JsonProcessingException e) {
            e.printStackTrace();
        }

        IndexRequest indexRequest = Requests.indexRequest();

        return indexRequest.create(false)
                .index("dev_test_5_" + indexName.toLowerCase())
//                .type("_doc")
                .id(id)
                .source(json, XContentType.JSON);
    }

    private static RestHighLevelClient getClient(List<HttpHost> httpHosts, ESRequestInterceptor requestInterceptor) {
        RestClientBuilder builder = RestClient.builder(httpHosts.get(0));
        Header[] headers = {new BasicHeader("Authorization", "Basic PLACEHOLDER_BASIC_AUTH")};
        builder.setDefaultHeaders(headers);

        return new RestHighLevelClient(builder);
//        return new RestHighLevelClient(
//                RestClient.builder(httpHosts.get(0))
//                        .setHttpClientConfigCallback(callback -> callback.addInterceptorLast(
//                                requestInterceptor)));
    }

    public static String getId(DmsEventLoc event) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] dataBytes = md.digest(event.getData().toString().getBytes(StandardCharsets.UTF_8));
            byte[] dig = md.digest(dataBytes);
            StringBuilder hexStringBuffer = new StringBuilder();
            for (byte b : dig) {
                char[] hexDigits = new char[2];
                hexDigits[0] = Character.forDigit((b >> 4) & 0xF, 16);
                hexDigits[1] = Character.forDigit((b & 0xF), 16);
                String hexString = new String(hexDigits);
                hexStringBuffer.append(hexString);
            }
            String id = hexStringBuffer.toString();
            return id;
        }
        catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }

        return null;
    }

    @SuppressWarnings({ "unused", "DuplicatedCode" })
    static class ESRequestInterceptor implements HttpRequestInterceptor, Serializable {
        private static final String ES_SERVICE_NAME = "es";
        private transient AWSSigningRequestInterceptor requestInterceptor;

        @Override
        public void process(HttpRequest httpRequest, HttpContext httpContext) throws HttpException, IOException {
            if (requestInterceptor == null) {
                final Supplier<LocalDateTime> clock = () -> LocalDateTime.now(ZoneOffset.UTC);
                String region = "us-east-1";
                final AWSSigner awsSigner = new AWSSigner(defaultCredentials, region, ES_SERVICE_NAME, clock::get);

                requestInterceptor = new AWSSigningRequestInterceptor(awsSigner);
            }

            requestInterceptor.process(httpRequest, httpContext);
        }
    }
}
