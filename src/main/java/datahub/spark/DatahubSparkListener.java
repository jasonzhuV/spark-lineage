package datahub.spark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import datahub.shaded.org.apache.kafka.clients.producer.KafkaProducer;
import datahub.shaded.org.apache.kafka.clients.producer.ProducerConfig;
import datahub.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import datahub.shaded.org.apache.kafka.common.serialization.StringSerializer;
import datahub.spark.consumer.impl.McpEmitter;
import datahub.spark.model.*;
import datahub.spark.model.dataset.SparkDataset;
import datahub.spark.model.kafka.Message;
import datahub.spark.model.kafka.QueryTable;
import datahub.spark.model.kafka.QueryTableInfo;
import datahub.spark.model.sinkbased.SinkBasedAppEndEvent;
import datahub.spark.model.sinkbased.SinkBasedAppStartEvent;
import datahub.spark.model.sinkbased.SinkBasedSqlQueryExecEndEvent;
import datahub.spark.model.sinkbased.SinkBasedSqlQueryExecStartEvent;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkEnv;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.QueryPlan;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SQLExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import scala.collection.JavaConversions;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractPartialFunction;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


/**
 * @author jason
 */
@Slf4j
public class DatahubSparkListener extends SparkListener {
    public static final String CONSUMER_TYPE_KEY = "spark.datahub.lineage.consumerTypes";
    public static final String DATAHUB_EMITTER = "McpEmitterBase";
    public static final String DATABRICKS_CLUSTER_KEY = "databricks.cluster";
    public static final String PIPELINE_KEY = "metadata.pipeline";
    public static final String PIPELINE_PLATFORM_INSTANCE_KEY = PIPELINE_KEY + ".platformInstance";
    public static final String COALESCE_KEY = "coalesce_jobs";
    private final Map<String, AppStartEvent> appDetails = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, AppStartEvent>> appRelateToTargetTable = new ConcurrentHashMap<>();
    private final Map<String, Map<Long, SQLQueryExecStartEvent>> appSqlDetails = new ConcurrentHashMap<>();
    private final Map<String, McpEmitter> appEmitters = new ConcurrentHashMap<>();
    private final Map<String, Config> appConfig = new ConcurrentHashMap<>();

    private KafkaProducer<String, String> producer;
    private static final Properties prop = new Properties();
    private static final String KAFKA_BROKERS = "172.41.4.87:9092,172.41.4.58:9092,172.41.4.71:9092";
    private static final String QUERY_RECORD = "ark-table-query";

    public DatahubSparkListener() {
        log.warn("==========> DatahubSparkListener initialised.");
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        prop.put("key.serializer", StringSerializer.class.getName());
        prop.put("value.serializer", StringSerializer.class.getName());
        prop.put("acks","all");
        prop.put("retries",0);
        prop.put("batch.size",16384);
        prop.put("linger.ms",1);
        prop.put("buffer.memory",33554432);
        producer = new KafkaProducer<>(prop);
    }

    public <T> void sendKafka(Message message, String topic) {
        ObjectMapper om = new ObjectMapper();
        try {
            producer.send(new ProducerRecord<String, String>(topic, om.writeValueAsString(message)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private class SqlStartTask {

        private final SparkListenerSQLExecutionStart sqlStart;
        private final SparkContext ctx;
        private final LogicalPlan plan;

        public SqlStartTask(SparkListenerSQLExecutionStart sqlStart, LogicalPlan plan, SparkContext ctx) {
            this.sqlStart = sqlStart;
            this.plan = plan;
            this.ctx = ctx;
        }

        public void run() {
            if (ctx == null) {
                log.error("Context is null skipping run");
                return;
            }
            if (ctx.conf() == null) {
                log.error("Context does not have config. Skipping run");
                return;
            }
            if (sqlStart == null) {
                log.error("sqlStart is null skipping run");
                return;
            }

//            Optional<? extends Collection<SparkDataset>> upStreamDatasets = DatasetExtractor.getUpStreamDatasets(plan, ctx);
//            upStreamDatasets.ifPresent(sparkDatasets -> sparkDatasets.forEach(d -> log.warn("==========> urn = {}", d.urn())));
//            log.warn("==========> user = {}", ctx.sparkUser());
//            log.warn("==========> timestamp = {}", ctx.startTime());

            Optional<? extends Collection<SparkDataset>> outputDs = DatasetExtractor.asDataset(plan, ctx, true);

            // Here assumption is that there will be only single target for single sql query
            DatasetLineage lineage = new DatasetLineage();
            lineage.setCallSiteShort(sqlStart.description());
            lineage.setPlan(plan.toString());
            Collection<QueryPlan<?>> allInners = new ArrayList<>();

            plan.collect(new AbstractPartialFunction<LogicalPlan, Void>() {
                @Override
                public Void apply(LogicalPlan plan) {
                    Optional<? extends Collection<SparkDataset>> inputDs = DatasetExtractor.asDataset(plan, ctx, false);
                    inputDs.ifPresent(x -> x.forEach(lineage::addSource));
                    allInners.addAll(JavaConversions.asJavaCollection(plan.innerChildren()));
                    return null;
                }
                @Override
                public boolean isDefinedAt(LogicalPlan x) {
                    return true;
                }
            });
            for (QueryPlan<?> qp : allInners) {
                if (!(qp instanceof LogicalPlan)) {
                    continue;
                }
                LogicalPlan nestedPlan = (LogicalPlan) qp;
                nestedPlan.collect(new AbstractPartialFunction<LogicalPlan, Void>() {
                    @Override
                    public Void apply(LogicalPlan plan) {
                        Optional<? extends Collection<SparkDataset>> inputDs = DatasetExtractor.asDataset(plan, ctx, false);
                        inputDs.ifPresent(x -> x.forEach(lineage::addSource));
                        return null;
                    }
                    @Override
                    public boolean isDefinedAt(LogicalPlan x) {
                        return true;
                    }
                });
            }

            try {
                lineage.getSources().forEach(d -> {
                    QueryTableInfo queryTableInfo = new QueryTableInfo();
                    queryTableInfo.setQueryText(lineage.getCallSiteShort());
                    queryTableInfo.setTimestamp(sqlStart.time());
                    queryTableInfo.setUser(ctx.sparkUser());
                    queryTableInfo.setEngine("spark");
                    queryTableInfo.setOperationName("QUERY");
                    queryTableInfo.setQueryTable(new QueryTable(d.urn().getDatasetNameEntity().split("\\.")[0], d.urn().getDatasetNameEntity().split("\\.")[1]));
                    sendKafka(queryTableInfo, QUERY_RECORD);
                });
            } catch (Exception e) {
                log.error("==========> 访问记录埋点报错.");
            }


            if (!outputDs.isPresent() || outputDs.get().isEmpty()) {
                return;
            }
            lineage.setSink(outputDs.get().iterator().next());
            McpEmitter emitter = appEmitters.get(ctx.applicationId());
            if (emitter != null) {
                // application
                checkOrEmitApplication(ctx, emitter, lineage, sqlStart.executionId());
                // sql
                emitSqlExec(ctx, emitter, sqlStart, lineage);
            }
        }
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart applicationStart) {
        try {
            LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {
                @Override
                public Void apply(SparkContext sc) {
                    checkOrCreateApplicationSetup(sc);
                    return null;
                }
            });
            super.onApplicationStart(applicationStart);
        } catch (Exception e) {
            // log error, but don't impact thread
            StringWriter s = new StringWriter();
            PrintWriter p = new PrintWriter(s);
            e.printStackTrace(p);
            log.error(s.toString());
            p.close();
        }
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd applicationEnd) {
        try {
            LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {
                @Override
                public Void apply(SparkContext sc) {
                    appRelateToTargetTable.remove(sc.applicationId()).forEach((sqlQueryId,event) -> {
                        SinkBasedAppStartEvent start = (SinkBasedAppStartEvent) event;
                        if (start == null) {
                            log.error("Application end event received, but start event missing for appId " + sc.applicationId());
                        } else {
                            AppEndEvent evt = new SinkBasedAppEndEvent(LineageUtils.getMaster(sc), getPipelineName(sc), sc.applicationId(), applicationEnd.time(), start);
                            McpEmitter emitter = appEmitters.get(sc.applicationId());
                            emitter.accept(evt);
                            consumers().forEach(x -> {
                                x.accept(evt);
                                try {
                                    x.close();
                                } catch (IOException e) {
                                    log.warn("Failed to close lineage consumer", e);
                                }
                            });
                        }
                    });
                    if (appEmitters.get(sc.applicationId()) != null) {
                        try {
                            appEmitters.get(sc.applicationId()).close();
                            appEmitters.remove(sc.applicationId());
                        } catch (Exception e) {
                            log.warn("Failed to close underlying emitter due to " + e.getMessage());
                        }
                    }
                    return null;
                }
            });
            super.onApplicationEnd(applicationEnd);
        } catch (Exception e) {
            // log error, but don't impact thread
            StringWriter s = new StringWriter();
            PrintWriter p = new PrintWriter(s);
            e.printStackTrace(p);
            log.error(s.toString());
            p.close();
        }
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
        try {
            if (event instanceof SparkListenerSQLExecutionStart) {
                SparkListenerSQLExecutionStart sqlEvt = (SparkListenerSQLExecutionStart) event;
                processExecution(sqlEvt);
            } else if (event instanceof SparkListenerSQLExecutionEnd) {
                SparkListenerSQLExecutionEnd sqlEvt = (SparkListenerSQLExecutionEnd) event;
                processExecutionEnd(sqlEvt);
            }
        } catch (Exception e) {
            // log error, but don't impact thread
            StringWriter s = new StringWriter();
            PrintWriter p = new PrintWriter(s);
            e.printStackTrace(p);
            log.error(s.toString());
            p.close();
        }
    }

    public void processExecutionEnd(SparkListenerSQLExecutionEnd sqlEnd) {
        LineageUtils.findSparkCtx().foreach(new AbstractFunction1<SparkContext, Void>() {
            @Override
            public Void apply(SparkContext sc) {
                SinkBasedSqlQueryExecStartEvent start = (SinkBasedSqlQueryExecStartEvent) appSqlDetails.get(sc.applicationId()).remove(sqlEnd.executionId());
                if (start == null) {
//                    log.warn("Execution end event received, but start event missing for appId/sql exec Id " + sc.applicationId() + ":" + sqlEnd.executionId());
                } else if (start.getDatasetLineage() != null) {
                    SinkBasedSqlQueryExecEndEvent evt = new SinkBasedSqlQueryExecEndEvent(
                            LineageUtils.getMaster(sc),
                            sc.appName(),
                            sc.applicationId(),
                            sqlEnd.time(),
                            sqlEnd.executionId(),
                            start);
                    McpEmitter emitter = appEmitters.get(sc.applicationId());
                    if (emitter != null) {
                        emitter.accept(evt);
                    }
                }
                return null;
            }
        });
    }

    private synchronized void checkOrCreateApplicationSetup(SparkContext ctx) {
        String appId = ctx.applicationId();
        Config datahubConfig = appConfig.get(appId);
        if (datahubConfig == null) {
            Config datahubConf = LineageUtils.parseSparkConfig();
            appConfig.put(appId, datahubConf);
            appEmitters.computeIfAbsent(appId, s -> new McpEmitter(datahubConf));
            appSqlDetails.put(appId, new ConcurrentHashMap<>());
            appRelateToTargetTable.put(appId, new ConcurrentHashMap<>());
        }
    }


    /**
     * emitApplication
     * if exists output dataset
     * @param ctx context
     */
    private synchronized void checkOrEmitApplication(SparkContext ctx, McpEmitter emitter, DatasetLineage datasetLineage, Long sqlQueryExecId) {
        String appId = ctx.applicationId();
        SparkDataset sink = datasetLineage.getSink();
        log.warn("==========> sinkDatasetNameEntity = {}", sink.urn().getDatasetNameEntity());
        Config datahubConf = appConfig.get(ctx.applicationId());
        Config pipelineConfig = datahubConf.hasPath(PIPELINE_KEY) ? datahubConf.getConfig(PIPELINE_KEY) : com.typesafe.config.ConfigFactory.empty();
        SinkBasedAppStartEvent event = new SinkBasedAppStartEvent(LineageUtils.getMaster(ctx), getPipelineName(ctx), appId, ctx.startTime(), ctx.sparkUser(), pipelineConfig, datasetLineage);
        emitter.accept(event);
        appRelateToTargetTable.get(appId).put(sqlQueryExecId, event);
    }

    private synchronized void emitSqlExec(SparkContext ctx, McpEmitter emitter, SparkListenerSQLExecutionStart sqlStart, DatasetLineage lineage) {
        SinkBasedSqlQueryExecStartEvent evt = new SinkBasedSqlQueryExecStartEvent(
                ctx.conf().get("spark.master"),
                getPipelineName(ctx),
                ctx.applicationId(),
                sqlStart.time(),
                sqlStart.executionId(),
                lineage
        );
        appSqlDetails.get(ctx.applicationId()).put(sqlStart.executionId(), evt);
        emitter.accept(evt);
        consumers().forEach(c -> c.accept(evt));
    }

    private String getPipelineName(SparkContext cx) {
        Config datahubConfig = appConfig.computeIfAbsent(cx.applicationId(), s -> LineageUtils.parseSparkConfig());
        String name = "";
        if (datahubConfig.hasPath(DATABRICKS_CLUSTER_KEY)) {
            name = datahubConfig.getString(DATABRICKS_CLUSTER_KEY) + "_" + cx.applicationId();
        }
        name = cx.appName();
        // TODO: appending of platform instance needs to be done at central location
        // like adding constructor to dataflowurl
        if (datahubConfig.hasPath(PIPELINE_PLATFORM_INSTANCE_KEY)) {
            name = datahubConfig.getString(PIPELINE_PLATFORM_INSTANCE_KEY) + "." + name;
        }
        return name;
    }

    private void processExecution(SparkListenerSQLExecutionStart sqlStart) {
        QueryExecution queryExec = SQLExecution.getQueryExecution(sqlStart.executionId());
        if (queryExec == null) {
//            log.warn("Skipping processing for sql exec Id " + sqlStart.executionId() + " as Query execution context could not be read from current spark state");
            return;
        }
        LogicalPlan plan = queryExec.optimizedPlan();
        SparkSession sess = queryExec.sparkSession();
        SparkContext ctx = sess.sparkContext();

        checkOrCreateApplicationSetup(ctx);

        (new SqlStartTask(sqlStart, plan, ctx)).run();
    }

    private List<LineageConsumer> consumers() {
        SparkConf conf = SparkEnv.get().conf();
        if (conf.contains(CONSUMER_TYPE_KEY)) {
            String consumerTypes = conf.get(CONSUMER_TYPE_KEY);
            return StreamSupport.stream(Splitter.on(",").trimResults().split(consumerTypes).spliterator(), false)
                    .map(x -> LineageUtils.getConsumer(x))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }
}
