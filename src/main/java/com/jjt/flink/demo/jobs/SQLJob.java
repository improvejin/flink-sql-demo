package com.jjt.flink.demo.jobs;

import com.jjt.flink.demo.sink.TableSinkFactory;
import com.jjt.flink.demo.source.KafkaTableSource;
import com.jjt.flink.demo.utils.UDFRegister;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.StreamTableSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

//import org.apache.flink.types.Row;


public class SQLJob {

    private static final Logger LOG = LoggerFactory.getLogger(SQLJob.class);

    public static void main(String[] args){
        LOG.info("args: {}", StringUtils.join(args, ' '));
        JobConfig jobConfig = new JobConfig(args);
        try {
            run(jobConfig);
        } catch (Exception e) {
            e.printStackTrace();
            printUsage();
        }
    }

    public static void run(JobConfig jobConfig) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        int parallelism = jobConfig.getParallelism();
        if(parallelism > 0) {
            env.setParallelism(jobConfig.getParallelism());
        }
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(5), CheckpointingMode.AT_LEAST_ONCE);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));
        //env.getConfig().setAutoWatermarkInterval(2000);
        StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);
        UDFRegister.registerUDF(tEnv);
        String sources = jobConfig.getSources();
        for(String source: sources.split(",")) {
            Class topicSchema = jobConfig.getTopicSchema(source);
            StreamTableSource tableSource = new KafkaTableSource(source, jobConfig, topicSchema);
            tEnv.registerTableSource(source, tableSource);
        }

        String sql = jobConfig.getSql().replace("`", "'");

        LOG.info("sql: {}", sql);
        Table result = tEnv.sqlQuery(sql);
        //tEnv.toAppendStream(result, Row.class).print();
        String[] outputs = jobConfig.getOutput().split(",");
        for(String output: outputs) {
            result.writeToSink(TableSinkFactory.createTableSink(jobConfig,output));
        }
        if(jobConfig.printPlan()) {
            System.out.println(env.getExecutionPlan());
        } else {
            env.execute(jobConfig.getName());
        }
    }


    public static void printUsage() {
        System.err.println(USAGE);
    }

    private static final String USAGE
            = "Parameters:\n"
            + "  --name[required] <job_name>\n"
            + "  --topic[required] <topic>\n"
            + "  --sql/file[required] <job_sql>/<job_sql_file>\n"
            + "  --parallelism <job_parallelism>\n"
            + "  --latency <latency>\n"
            + "  --output <console/kafka.topic/mysql.table>\n"
            + "  --plan\n";
}
