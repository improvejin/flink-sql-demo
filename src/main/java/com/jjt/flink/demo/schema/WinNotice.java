package com.jjt.flink.demo.schema;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;

public class WinNotice extends AbstractSchema<WinNotice> {
    public Timestamp ts; //rowtime
    public Long date;
    public Long hour;
    public Long minutesec;
    public String reqid;
    public String supplierid;
    public String publisherid;
    public String impid;
    public String cpid;
    public String adid;
    public String crid;
    public String uid;
    public Long price;
    public Long bid_price;
    public String checksum;
    public String alg_tag;
    public Map<String,String> ext;
    public String cost_type;
    public Long risk;
    public String risk_reason;
    public String host;

    @Override
    public WinNotice deserialize(byte[] bytes) throws IOException {
        WinNotice winNotice = super.deserialize(bytes);
        return winNotice;
    }

    @Override
    public String getTimeStampAttribute() {
        return "ts";
    }

    @Override
    public String toString() {
        return "WinNotice{" +
                "ts=" + ts +
                ", date=" + date +
                ", hour=" + hour +
                ", minutesec=" + minutesec +
                ", reqid='" + reqid + '\'' +
                ", supplierid='" + supplierid + '\'' +
                ", publisherid='" + publisherid + '\'' +
                ", impid='" + impid + '\'' +
                ", cpid='" + cpid + '\'' +
                ", adid='" + adid + '\'' +
                ", crid='" + crid + '\'' +
                ", uid='" + uid + '\'' +
                ", price=" + price +
                ", bid_price=" + bid_price +
                ", checksum='" + checksum + '\'' +
                ", alg_tag='" + alg_tag + '\'' +
                ", ext=" + ext +
                ", cost_type='" + cost_type + '\'' +
                ", risk=" + risk +
                ", risk_reason='" + risk_reason + '\'' +
                ", host='" + host + '\'' +
                '}';
    }
}
