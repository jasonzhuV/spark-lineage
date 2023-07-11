package datahub.spark.model;

import com.linkedin.common.DatasetUrnArray;
import com.linkedin.common.urn.DataFlowUrn;
import com.linkedin.common.urn.DataJobUrn;
import com.linkedin.data.template.StringMap;
import datahub.shaded.com.google.common.base.Joiner;
import datahub.spark.model.dataset.SparkDataset;
import com.linkedin.datajob.DataJobInfo;
import com.linkedin.datajob.DataJobInputOutput;
import com.linkedin.datajob.JobStatus;
import datahub.event.MetadataChangeProposalWrapper;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;


@ToString
@Getter
@Slf4j
public class SQLQueryExecStartEvent extends LineageEvent {
    private final long sqlQueryExecId;
    private final DatasetLineage datasetLineage;

    private Set<String> sourceNames;

    public SQLQueryExecStartEvent(String master, String appName, String appId, long time, long sqlQueryExecId,
                                  DatasetLineage datasetLineage) {
        super(master, appName, appId, time);
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
        log.warn("==========> SQLQueryExecStartEvent.asMetadataEvents is called");
        DataJobUrn jobUrn = jobUrn();
        MetadataChangeProposalWrapper mcpJobIo =
                MetadataChangeProposalWrapper.create(b -> b.entityType("dataJob").entityUrn(jobUrn).upsert().aspect(jobIO()));

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
        return String.join("|", sourceNames);
    }

    DataJobUrn jobUrn() {
        /* This is for generating urn from a hash of the plan */
//        Set<String> sourceUrns = datasetLineage.getSources().parallelStream().map(x -> x.urn().toString()).collect(Collectors.toSet());
//        sourceUrns = new TreeSet<>(sourceUrns); //sort for consistency
//        String sinkUrn = datasetLineage.getSink().urn().toString();
//        String plan = LineageUtils.scrubPlan(datasetLineage.getPlan());
//        String id = Joiner.on(",").join(sinkUrn, sourceUrns, plan);
//        return new DataJobUrn(flowUrn(), "planHash_" + LineageUtils.hash(id));
        return new DataJobUrn(flowUrn(), "QueryExecId_" + sqlQueryExecId);
    }

    DataFlowUrn flowUrn() {
        return LineageUtils.flowUrn(getMaster(), getAppName());
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