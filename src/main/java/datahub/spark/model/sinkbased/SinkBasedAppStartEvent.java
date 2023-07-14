package datahub.spark.model.sinkbased;

import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataFlowInfo;
import com.typesafe.config.Config;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.AppStartEvent;
import datahub.spark.model.DatasetLineage;
import datahub.spark.model.LineageUtils;
import datahub.spark.model.TableInfo;
import datahub.spark.model.dataset.SparkDataset;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.*;

@Slf4j
@ToString
@Getter
@Setter
public class SinkBasedAppStartEvent extends AppStartEvent {

    private static final String PLATFORM_INSTANCE_KEY = "platformInstance";
    private static final String PLATFORM_SPARK = "spark";
    private final String sparkUser;
    private Config pipelineConfig;
    private DatasetLineage datasetLineage;
    private final TableInfo sinkTableInfo;
    private DataFlowUrn flowUrn;

    public SinkBasedAppStartEvent(String master, String appName, String appId, long time, String sparkUser, Config pipelineConfig, DatasetLineage datasetLineage) {
        super(master, appName, appId, time, sparkUser, pipelineConfig);
        this.sparkUser = sparkUser;
        this.pipelineConfig = pipelineConfig;
        this.datasetLineage = datasetLineage;
        this.sinkTableInfo = new TableInfo();
        SparkDataset sink = datasetLineage.getSink();
        String dbName = sink.urn().getDatasetNameEntity().split("\\.")[0];
        String tableName = sink.urn().getDatasetNameEntity().split("\\.")[1];
        if (StringUtils.isNotEmpty(dbName) && StringUtils.isNotEmpty(tableName)) {
            sinkTableInfo.setDbName(dbName);
            sinkTableInfo.setTableName(tableName);
        }
    }

    public DataFlowUrn getFlowUrn() {
        if (StringUtils.isNotEmpty(sinkTableInfo.getDbName()) && StringUtils.isNotEmpty(sinkTableInfo.getTableName())) {
            return LineageUtils.flowUrn(sinkTableInfo.getDbName(), sinkTableInfo.getTableName());
        } else {
            return LineageUtils.flowUrn(getMaster(), getAppName());
        }
    }

    @Override
    public List<MetadataChangeProposalWrapper> asMetadataEvents() {
        log.warn("==========> SinkBasedAppStartEvent.asMetadataEvents is called");
        ArrayList<MetadataChangeProposalWrapper> mcps = new ArrayList<MetadataChangeProposalWrapper>();
        DataFlowUrn flowUrn = getFlowUrn();
        this.flowUrn = flowUrn;
        mcps.add(MetadataChangeProposalWrapper.create(b ->
               b.entityType("dataFlow")
                .entityUrn(flowUrn)
                .upsert()
                .aspect(new DataFlowInfo().setName(sinkTableInfo.getTableName()).setCustomProperties(customProps()))
        ));
        return mcps;
    }

    StringMap customProps() {
        StringMap customProps = new StringMap();
        customProps.put("startedAt", timeStr());
        customProps.put("appId", getAppId());
        customProps.put("appName", getAppName());
        customProps.put("sparkUser", sparkUser);
        return customProps;
    }
}