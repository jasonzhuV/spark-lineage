package datahub.spark.consumer.impl;

import com.typesafe.config.Config;
import datahub.client.Emitter;
import datahub.client.rest.RestEmitter;
import datahub.client.rest.RestEmitterConfig;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.LineageConsumer;
import datahub.spark.model.LineageEvent;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@Slf4j
public class McpEmitterBak implements LineageConsumer {
//  private static Logger log = Logger.getLogger(McpEmitter.class);

  private String emitterType;
  private Optional<RestEmitterConfig> restEmitterConfig;
  private static final String TRANSPORT_KEY = "transport";
  private static final String GMS_URL_KEY = "rest.server";
  private static final String GMS_AUTH_TOKEN = "rest.token";
  private static final String DISABLE_SSL_VERIFICATION_KEY = "rest.disable_ssl_verification";
  private Optional<Emitter> getEmitter() {
    Optional<Emitter> emitter = Optional.empty();
    switch (emitterType) {
    case "rest":
      if (restEmitterConfig.isPresent()) {
        emitter = Optional.of(new RestEmitter(restEmitterConfig.get()));
      }
      break;
      
    default:
      log.error("DataHub Transport " +emitterType +" not recognized. DataHub Lineage emission will not work");
      break;

    }
    return emitter;
  }

  protected void emit(List<MetadataChangeProposalWrapper> mcpws) {
    Optional<Emitter> emitter = getEmitter();
    //log.warn("===============>  McpEmitter.emit");
    if (emitter.isPresent()) {
      //log.warn("===============>  McpEmitter.emit isPresent");
      mcpws.stream().map(mcpw -> {
        try {
          log.debug("emitting mcpw: " + mcpw);
          //log.warn("===============>  emitting mcpw = " + mcpw);
          return emitter.get().emit(mcpw);
        } catch (IOException ioException) {
          log.error("Failed to emit metadata to DataHub", ioException);
          return null;
        }
      }).filter(Objects::nonNull).collect(Collectors.toList()).forEach(future -> {
        try {
          log.info(future.get().toString());
        } catch (InterruptedException | ExecutionException e) {
          // log error, but don't impact thread
          log.error("Failed to emit metadata to DataHub", e);
        }
      });
      try {
        emitter.get().close();
      } catch (IOException e) {
        log.error("Issue while closing emitter" + e);
      }
    }
  }

  public McpEmitterBak(Config datahubConf) {
      //log.warn("===============>  创建 McpEmitter");
      //log.warn("===============>  创建 McpEmitter");
      emitterType = datahubConf.hasPath(TRANSPORT_KEY) ? datahubConf.getString(TRANSPORT_KEY) : "rest";
      switch (emitterType) {
      case "rest":
          String gmsUrl = datahubConf.hasPath(GMS_URL_KEY) ? datahubConf.getString(GMS_URL_KEY) : "http://localhost:8080";
          String token = datahubConf.hasPath(GMS_AUTH_TOKEN) ? datahubConf.getString(GMS_AUTH_TOKEN) : null;
          boolean disableSslVerification = datahubConf.hasPath(DISABLE_SSL_VERIFICATION_KEY) ? datahubConf.getBoolean(DISABLE_SSL_VERIFICATION_KEY) : false;
//          log.info("REST Emitter Configuration: GMS url {}{}", gmsUrl, (datahubConf.hasPath(GMS_URL_KEY) ? "" : "(default)"));
          if (token != null) {
//              log.info("REST Emitter Configuration: Token {}", (token != null) ? "XXXXX" : "(empty)");
          }
          if (disableSslVerification) {
            //log.warn("REST Emitter Configuration: ssl verification will be disabled.");
          }
          restEmitterConfig = Optional.of(RestEmitterConfig.builder()
              .server(gmsUrl).token(token)
              .disableSslVerification(disableSslVerification).build());

          break;
      default:
          log.error("DataHub Transport " +emitterType+ " not recognized. DataHub Lineage emission will not work" );
          break;
      }
  }

  @Override
  public void accept(LineageEvent evt) {
    //log.warn("===============>  McpEmitter.accept");
    emit(evt.asMetadataEvents());
  }

  @Override
  public void close() throws IOException {
    // Nothing to close at this point
    
  }

 
}