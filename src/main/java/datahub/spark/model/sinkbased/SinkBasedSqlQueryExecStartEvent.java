package datahub.spark.model.sinkbased;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.JobStatus;
import datahub.event.MetadataChangeProposalWrapper;
import datahub.spark.model.*;
import datahub.spark.model.dataset.SparkDataset;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;

import java.util.*;


@ToString
@Getter
@Slf4j
public class SinkBasedSqlQueryExecStartEvent extends SQLQueryExecStartEvent {
    private final long sqlQueryExecId;
    private final DatasetLineage datasetLineage;
    private Set<String> sourceNames;

    public SinkBasedSqlQueryExecStartEvent(String master, String appName, String appId, long time, long sqlQueryExecId, DatasetLineage datasetLineage) {
        super(master, appName, appId, time, sqlQueryExecId, datasetLineage);
        this.sqlQueryExecId = sqlQueryExecId;
        this.datasetLineage = datasetLineage;
        sourceNames = new TreeSet<>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
    }

    @Override
    public List<MetadataChangeProposalWrapper> asMetadataEvents() {
        log.warn("==========> SinkBasedSqlQueryExecStartEvent.asMetadataEvents is called");
        DataJobInputOutput jobIo = jobIO();
        DataJobUrn jobUrn = jobUrn();
        MetadataChangeProposalWrapper mcpJobIo =
                MetadataChangeProposalWrapper.create(b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobIo));

        DataJobInfo jobInfo = jobInfo();
        jobInfo.setCustomProperties(customProps());
        jobInfo.setStatus(JobStatus.IN_PROGRESS);

        MetadataChangeProposalWrapper mcpJobInfo =
                MetadataChangeProposalWrapper.create(b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobInfo));

        return Arrays.asList(mcpJobIo, mcpJobInfo);
    }

    DataJobInfo jobInfo() {
//    return new DataJobInfo().setName(datasetLineage.getCallSiteShort()).setType(DataJobInfo.Type.create("sparkJob"));
        return new DataJobInfo().setName(dataJobName()).setType(DataJobInfo.Type.create("sparkJob"));
    }

    String dataJobName() {
        return String.join("^", sourceNames);
    }

    DataJobUrn jobUrn() {
        /* This is for generating urn from a hash of the plan */
//        Set<String> sourceUrns = datasetLineage.getSources().parallelStream().map(x -> x.urn().toString()).collect(Collectors.toSet());
//        sourceUrns = new TreeSet<>(sourceUrns); //sort for consistency
//        String sinkUrn = datasetLineage.getSink().urn().toString();
//        String plan = LineageUtils.scrubPlan(datasetLineage.getPlan());
//        String id = Joiner.on(",").join(sinkUrn, sourceUrns, plan);
//        return new DataJobUrn(flowUrn(), "planHash_" + LineageUtils.hash(id));
        return new DataJobUrn(flowUrn(), String.valueOf(sourceNames.hashCode() & Integer.MAX_VALUE));
    }

    DataFlowUrn flowUrn() {
        SparkDataset sink = datasetLineage.getSink();
        String dbName = sink.urn().getDatasetNameEntity().split("\\.")[0];
        String tableName = sink.urn().getDatasetNameEntity().split("\\.")[1];
        if (StringUtils.isNotEmpty(dbName) && StringUtils.isNotEmpty(tableName)) {
            return LineageUtils.flowUrn(dbName, tableName);
        } else {
            return LineageUtils.flowUrn(getMaster(), getAppName());
        }
    }

    StringMap customProps() {
        StringMap customProps = new StringMap();
        customProps.put("startedAt", timeStr());
        customProps.put("description", datasetLineage.getCallSiteShort());
        customProps.put("SQLQueryId", Long.toString(sqlQueryExecId));
        customProps.put("appId", getAppId());
        customProps.put("appName", getAppName());
//        customProps.put("queryPlan", datasetLineage.getPlan());
        return customProps;
    }

    public DatasetUrnArray getOuputDatasets() {
        DatasetUrnArray out = new DatasetUrnArray();
        out.add(datasetLineage.getSink().urn());
        return out;
    }

    public DatasetUrnArray getInputDatasets() {
        DatasetUrnArray in = new DatasetUrnArray();


        Set<SparkDataset> sources = new TreeSet<>(new Comparator<SparkDataset>() {
            @Override
            public int compare(SparkDataset x, SparkDataset y) {
                return x.urn().toString().compareTo(y.urn().toString());
            }
        });
        // maintain ordering
        sources.addAll(datasetLineage.getSources());
        for (SparkDataset source : sources) {
            in.add(source.urn());
            sourceNames.add(source.urn().getDatasetNameEntity());
        }

        return in;
    }

    private DataJobInputOutput jobIO() {
        DataJobInputOutput io = new DataJobInputOutput()
                .setInputDatasets(getInputDatasets())
                .setOutputDatasets(getOuputDatasets());
        return io;
    }
}