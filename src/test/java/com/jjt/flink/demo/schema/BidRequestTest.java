package com.jjt.flink.demo.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.junit.Ignore;

public class BidRequestTest {

    @Ignore
    public void test() {
        TypeInformation<BidRequest> t = TypeInformation.of(BidRequest.class);
        System.out.println(t);
    }
}
