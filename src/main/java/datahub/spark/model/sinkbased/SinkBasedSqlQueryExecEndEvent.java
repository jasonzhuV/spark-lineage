package datahub.spark.model.sinkbased;

import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.LineageEvent;
import datahub.spark.model.SQLQueryExecEndEvent;
import datahub.spark.model.SQLQueryExecStartEvent;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;


@ToString
@Getter
@Slf4j
public class SinkBasedSqlQueryExecEndEvent extends SQLQueryExecEndEvent {

  private final long sqlQueryExecId;
  private final SinkBasedSqlQueryExecStartEvent start;

  public SinkBasedSqlQueryExecEndEvent(String master, String appName, String appId, long time, long sqlQueryExecId, SinkBasedSqlQueryExecStartEvent start) {
    super(master, appName, appId, time, sqlQueryExecId, start);
    this.sqlQueryExecId = sqlQueryExecId;
    this.start = start;
  }

  @Override
  public List<MetadataChangeProposalWrapper> asMetadataEvents() {
    log.warn("==========> SinkBasedSqlQueryExecEndEvent.asMetadataEvents is called");
    DataJobUrn jobUrn = start.jobUrn();
    StringMap customProps = start.customProps();
    customProps.put("completedAt", timeStr());

    DataJobInfo jobInfo = start.jobInfo().setCustomProperties(customProps);

    return Collections.singletonList(
        MetadataChangeProposalWrapper.create(
            b -> b.entityType("dataJob")
                  .entityUrn(jobUrn)
                  .upsert()
                  .aspect(jobInfo)
        )
    );
  }
}