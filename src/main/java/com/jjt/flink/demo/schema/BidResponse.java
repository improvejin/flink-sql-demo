package com.jjt.flink.demo.schema;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class BidResponse extends AbstractSchema<BidResponse> {


    @Override
    public String getTimeStampAttribute() {
        return null;
    }

    @Override
    public BidResponse deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public TypeInformation<BidResponse> getProducedType() {
        return null;
    }
}
