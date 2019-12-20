package com.jjt.flink.demo.jobs;

import org.junit.Test;

public class JobConfigTest {

    @Test
    public void test1() {
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--file",
                "hdfs:///a/b/c.txt" };
        JobConfig jobConfig = new JobConfig(args);
        jobConfig.getSql();
    }
}
