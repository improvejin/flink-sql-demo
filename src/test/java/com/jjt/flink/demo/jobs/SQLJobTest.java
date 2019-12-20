package com.jjt.flink.demo.jobs;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Test;

public class SQLJobTest {

    @Test
    public void test1(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select supplierid, TUMBLE_END(ts, INTERVAL '10' SECOND) as ts , count(distinct impid) as impression_count" +
                        " from bidrequest group by TUMBLE(ts, INTERVAL '10' SECOND), supplierid", "--output", "mysql.sqltest",
                "--mysql.batch-size", "20"};
        SQLJob.main(args);
    }

    @Test
    public void test2(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select ts, supplierid, app_category, app_category[1] as x1,  ext, ext['ct'] as x2, ext['fl1'] as x3 from bidrequest where app_category is not null"};
        SQLJob.main(args);
    }

    @Test
    public void test3(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select ts, pids ,x, host,  substring(host from 1 for 3) as idc from bidrequest,  LATERAL TABLE(explode(pids)) as t(x) where pids is not null "};
        SQLJob.main(args);
    }

    @Test
    public void test4(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest","--plan", "--sql",
                "select ts, ext['ct'], split(ext['ct'], '_'), cast(split(ext['ct'], '_')[2] AS int) from bidrequest where ext['ct'] is not null" };
        SQLJob.main(args);
    }

    @Test
    public void test5(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select ts, supplierid, pids, x, y from bidrequest, LATERAL TABLE(group_set2(supplierid,pids)) as t(x, y)" };
        SQLJob.main(args);
    }

    @Test
    public void test6(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select TUMBLE_ROWTIME(ts, INTERVAL '1' MINUTE) as ts1, count(1) as c from bidrequest group by TUMBLE(ts, INTERVAL '1' MINUTE),RAND_INTEGER(5)" };
        SQLJob.main(args);
    }

    @Test
    public void test7(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select TUMBLE_END(ts1, INTERVAL '1' MINUTE) , sum(CAST(c as BIGINT)) from (select TUMBLE_ROWTIME(ts, INTERVAL '1' MINUTE) as ts1, count(1) as c from bidrequest group by TUMBLE(ts, INTERVAL '1' MINUTE),RAND_INTEGER(5))t group by TUMBLE(ts1, INTERVAL '1' MINUTE)" };
        SQLJob.main(args);
    }

    @Test
    public void test8(){
        String[] args = new String[]{"--topic", "bidrequest", "--name", "sqltest", "--sql",
                "select supplierid as hkey, ad_format as x__a, ip from bidrequest",
                "--output", "hbase.sqltest", "--parallelism", "1" };
        SQLJob.main(args);
    }


    @Test
    public void test9(){
        String[] args = new String[]{"--sources", "impression,winnotice", "--name", "sqltest", "--sql",
                "select TUMBLE_START(ts, INTERVAL '5' MINUTE) as ts,cpid,adid,crid,\n" +
                        "count(distinct case when event_type='st'then impid else null end) as impression_count,\n" +
                        "count(distinct case when event_type='cl'then impid else null end) as click_count,\n" +
                        "sum(price) as cost\n" +
                        "from(select ts, cpid,adid,crid, event_type, impid,\n" +
                        "case when event_type='st' and cost_type = 'icpm' and  price < 10000000 then price when event_type='cl' and cost_type = 'cpc' then price*1000 else 0 end  as price\n" +
                        "from impression\n" +
                        "where risk<2 and price>0 and (event_type='st' or event_type='cl')\n" +
                        "union all select ts, cpid,adid,crid, '' as event_type, '' as impid, price \n" +
                        "from winnotice\n" +
                        "where risk<2 and  cost_type='wcpm' and price>0) group by TUMBLE(ts, INTERVAL '5' MINUTE), cpid,adid,crid",
                 "--parallelism", "1" , "--plan"};
        SQLJob.main(args);
    }

}
