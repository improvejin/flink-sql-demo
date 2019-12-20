package com.jjt.flink.demo.jobs;

import com.jjt.flink.demo.utils.Constants;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Properties;

public class JobConfig {

    private static final Logger logger = LoggerFactory.getLogger(JobConfig.class);

    private ParameterTool cliParameter;

    private ParameterTool fileParameter;

    public JobConfig(String[] args) {
        InputStream in = JobConfig.class.getClassLoader().getResourceAsStream("job.properties");
        try {
            this.fileParameter = ParameterTool.fromPropertiesFile(in);
        } catch (IOException e) {
            logger.error("can not read properties from class path file: job.properties, msg: {}",  e.getMessage());
        }
        this.cliParameter = ParameterTool.fromArgs(args);
    }


    public String getSources() {
        return cliParameter.getRequired("topic");
    }

    public String getTopicFromFile(String source) {
        return fileParameter.getRequired("kafka.topic." + source);
    }

    public Class getTopicSchema(String topic) throws ClassNotFoundException {
        String schema = fileParameter.getRequired("schema.topic." + topic);
        return Class.forName(schema);
    }

    public Properties getConsumerProp(String topic){
        Properties props =  new Properties();
        props.setProperty(Constants.KAFKA_BROKER, fileParameter.getRequired(Constants.KAFKA_BROKER));
        props.setProperty(Constants.KAFKA_ZK, fileParameter.getRequired(Constants.KAFKA_ZK));
        props.setProperty("group.id", String.format("flink-group-%s-%s", topic, this.getName()));
        return props;
    }

    public Properties getProducerProp() {
        Properties props =  new Properties();
        props.setProperty(Constants.KAFKA_BROKER, fileParameter.getRequired(Constants.KAFKA_BROKER));
        return props;
    }

    public String getName() {
        return cliParameter.getRequired("name");
    }

    public boolean printPlan() {
        return cliParameter.has("plan");
    }

    public int getParallelism() {
        return this.getInt("parallelism");
    }

    public String getSql() {
        if(cliParameter.has("sql")) {
            return cliParameter.getRequired("sql");
        } else {
            try {
                return getSqlFromHdfs(cliParameter.getRequired("file"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public int getLatency() {
        return getInt("latency", 60);
    }

    public int getInt(String key) {
        int defaultValue = fileParameter.getInt(key);
        int p = cliParameter.getInt(key, defaultValue);
        return p;
    }

    public int getInt(String key, int defaultValue) {
        int v = fileParameter.getInt(key, defaultValue);
        v = cliParameter.getInt(key, v);
        return v;
    }

    public String getFromFile(String key) {
        return fileParameter.getRequired(key);
    }

    public String getOutput() {
        return cliParameter.get("output", "console");
    }

    public Properties getDruidConnectionProps() {
        InputStream in = JobConfig.class.getClassLoader().getResourceAsStream("druid.properties");
        Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error("can not read properties from class path file: druid.properties, msg: {}",  e.getMessage());
            throw new RuntimeException(e);
        }
        return properties;
    }

    public String getSqlFromHdfs(String path) throws IOException{
        HadoopFsFactory fsFactory = new HadoopFsFactory();
        FileSystem fs = fsFactory.create(URI.create (path));
        FSDataInputStream in = fs.open(new Path(path));
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        StringBuilder sb = new StringBuilder();
        String s;
        while ((s = reader.readLine())!=null) {
           sb.append(s);
           sb.append("\n");
        }
        reader.close();
        in.close();
        return sb.toString();
    }
}
