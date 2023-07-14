package datahub.spark.model.sinkbased;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.AppEndEvent;
import datahub.spark.model.AppStartEvent;
import datahub.spark.model.LineageEvent;
import datahub.spark.model.LineageUtils;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;


@ToString
@Getter
@Slf4j
public class SinkBasedAppEndEvent extends AppEndEvent {

    private final SinkBasedAppStartEvent start;

    public SinkBasedAppEndEvent(String master, String appName, String appId, long time, SinkBasedAppStartEvent start) {
        super(master, appName, appId, time, start);
        this.start = start;
    }

    @Override
    public List<MetadataChangeProposalWrapper> asMetadataEvents() {
        log.warn("==========> SinkBasedAppEndEvent.asMetadataEvents is called");
        DataFlowUrn flowUrn = start.getFlowUrn();
        StringMap customProps = start.customProps();
        String sinkTableName = start.getSinkTableInfo().getTableName();
        customProps.put("completedAt", timeStr());
        DataFlowInfo flowInfo = new DataFlowInfo().setName(sinkTableName).setCustomProperties(customProps);
        return Collections.singletonList(MetadataChangeProposalWrapper.create(
                b -> b.entityType("dataFlow")
                      .entityUrn(flowUrn)
                      .upsert()
                      .aspect(flowInfo)
        ));
    }
}