package datahub.spark;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import datahub.spark.consumer.impl.McpEmitter;
import datahub.spark.model.*;
import datahub.spark.model.dataset.SparkDataset;
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
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
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
    private final Map<String, Map<Long, SQLQueryExecStartEvent>> appSqlDetails = new ConcurrentHashMap<>();
    private final Map<String, McpEmitter> appEmitters = new ConcurrentHashMap<>();
    private final Map<String, Config> appConfig = new ConcurrentHashMap<>();

    public DatahubSparkListener() {
        log.warn("DatahubSparkListener initialised.");
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

            appSqlDetails
                    .get(ctx.applicationId())
                    .put(sqlStart.executionId(), new SQLQueryExecStartEvent(
                            ctx.conf().get("spark.master"),
                            getPipelineName(ctx),
                            ctx.applicationId(),
                            sqlStart.time(),
                            sqlStart.executionId(),
                            null)
                    );

            Optional<? extends Collection<SparkDataset>> outputDs = DatasetExtractor.asDataset(plan, ctx, true);
            if (!outputDs.isPresent() || outputDs.get().isEmpty()) {
                return;
            }
            // Here assumption is that there will be only single target for single sql query
            DatasetLineage lineage = new DatasetLineage(sqlStart.description(), plan.toString(), outputDs.get().iterator().next());
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
            SQLQueryExecStartEvent evt = new SQLQueryExecStartEvent(
                    ctx.conf().get("spark.master"),
                    getPipelineName(ctx),
                    ctx.applicationId(),
                    sqlStart.time(),
                    sqlStart.executionId(),
                    lineage
            );

            appSqlDetails.get(ctx.applicationId()).put(sqlStart.executionId(), evt);

            McpEmitter emitter = appEmitters.get(ctx.applicationId());
            if (emitter != null) {
                emitter.accept(evt);
            }
            consumers().forEach(c -> c.accept(evt));
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
                    AppStartEvent start = appDetails.remove(sc.applicationId());
                    appSqlDetails.remove(sc.applicationId());
                    if (start == null) {
                        log.error("Application end event received, but start event missing for appId " + sc.applicationId());
                    } else {
                        AppEndEvent evt = new AppEndEvent(LineageUtils.getMaster(sc), getPipelineName(sc), sc.applicationId(),
                                applicationEnd.time(), start);

                        McpEmitter emitter = appEmitters.get(sc.applicationId());
                        if (emitter != null) {
                            emitter.accept(evt);
                            try {
                                emitter.close();
                                appEmitters.remove(sc.applicationId());
                            } catch (Exception e) {
                                log.warn("Failed to close underlying emitter due to " + e.getMessage());
                            }
                        }
                        consumers().forEach(x -> {
                            x.accept(evt);
                            try {
                                x.close();
                            } catch (IOException e) {
                                log.warn("Failed to close lineage consumer", e);
                            }
                        });
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
                SQLQueryExecStartEvent start = appSqlDetails.get(sc.applicationId()).remove(sqlEnd.executionId());
                if (start == null) {
//                    log.warn("Execution end event received, but start event missing for appId/sql exec Id " + sc.applicationId() + ":" + sqlEnd.executionId());
                } else if (start.getDatasetLineage() != null) {
                    SQLQueryExecEndEvent evt = new SQLQueryExecEndEvent(
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
        ExecutorService pool = null;
        String appId = ctx.applicationId();
        Config datahubConfig = appConfig.get(appId);
        if (datahubConfig == null) {
            Config datahubConf = LineageUtils.parseSparkConfig();
            appConfig.put(appId, datahubConf);
            Config pipelineConfig = datahubConf.hasPath(PIPELINE_KEY) ? datahubConf.getConfig(PIPELINE_KEY) : com.typesafe.config.ConfigFactory.empty();
            AppStartEvent evt = new AppStartEvent(LineageUtils.getMaster(ctx), getPipelineName(ctx), appId, ctx.startTime(), ctx.sparkUser(), pipelineConfig);
            appEmitters.computeIfAbsent(appId, s -> new McpEmitter(datahubConf)).accept(evt);
            consumers().forEach(c -> c.accept(evt));
            appDetails.put(appId, evt);
            appSqlDetails.put(appId, new ConcurrentHashMap<>());
        }
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
