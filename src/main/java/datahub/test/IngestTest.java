package datahub.test;

import com.linkedin.dataset.DatasetProperties;
import com.linkedin.events.metadata.ChangeType;
import datahub.client.Emitter;
import datahub.client.MetadataWriteResponse;
import datahub.client.rest.RestEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.client.rest.RestEmitter;
import datahub.client.Callback;
import org.apache.http.HttpResponse;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;


/**
 * @author : zhupeiwen
 * @date : 2023/7/14
 */
public class IngestTest {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        Optional<RestEmitterConfig> restEmitterConfig;
        String gmsUrl = "http://node2:58080";
        String token = null;
        boolean disableSslVerification = false;
        restEmitterConfig = Optional.of(
                RestEmitterConfig.builder()
                        .server(gmsUrl).token(token)
                        .disableSslVerification(disableSslVerification).build());
        Optional<Emitter>emitter = Optional.of(new RestEmitter(restEmitterConfig.get()));


        MetadataChangeProposalWrapper mcpw = MetadataChangeProposalWrapper.builder()
                .entityType("dataset")
                .entityUrn("urn:li:dataset:(urn:li:dataPlatform:hive,test_zhupeiwen.source_table,PROD)")
                .upsert()
                .aspect(new DatasetProperties().setDescription("This is the canonical User profile dataset"))
                .build();

        // Blocking call using future
//        MetadataWriteResponse requestFuture = emitter.emit(mcpw).get();

        // Non-blocking using callback
        emitter.get().emit(mcpw, new Callback() {
            @Override
            public void onCompletion(MetadataWriteResponse response) {
                if (response.isSuccess()) {
                    System.out.println(String.format("Successfully emitted metadata event for %s", mcpw.getEntityUrn()));
                } else {
                    // Get the underlying http response
                    HttpResponse httpResponse = (HttpResponse) response.getUnderlyingResponse();
                    System.out.println(String.format("Failed to emit metadata event for %s, aspect: %s with status code: %d",
                            mcpw.getEntityUrn(), mcpw.getAspectName(), httpResponse.getStatusLine().getStatusCode()));
                    // Print the server side exception if it was captured
                    if (response.getResponseContent() != null) {
                        System.out.println(String.format("Server side exception was %s", response.getResponseContent()));
                    }
                }
            }
            @Override
            public void onFailure(Throwable exception) {
                System.out.println(
                        String.format("Failed to emit metadata event for %s, aspect: %s due to %s", mcpw.getEntityUrn(),
                                mcpw.getAspectName(), exception.getMessage()));
            }
        });

    }
}
