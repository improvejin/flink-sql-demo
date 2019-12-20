package com.jjt.flink.demo.schema;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Map;

public class Impression extends AbstractSchema<Impression> {

    public Timestamp ts; //rowtime
    public Long date;
    public Long hour;
    public Long minutesec;
    public String ip;
    public String regionid;
    public String country;
    public String province;
    public String city;
    public String ispid;
    public String reqid;
    public String supplierid;
    public String publisherid;
    public String impid;
    public String cpid;
    public String adid;
    public String cost_type;
    public String crid;
    public String uid;
    public String event_type;
    public Long price;
    public Long bid_price;
    public String ua;
    public String ref_url;
    public Long risk;
    public String risk_reason;
    public String checksum;
    public String alg_tag;
    public String dmp_tag;
    public Map<String,String> ext;
    public String host;

    @Override
    public Impression deserialize(byte[] bytes) throws IOException {
        Impression impression = super.deserialize(bytes);
        return impression;
    }

    @Override
    public String getTimeStampAttribute() {
        return "ts";
    }

    @Override
    public String toString() {
        return "Impression{" +
                "ts=" + ts +
                ", date=" + date +
                ", hour=" + hour +
                ", minutesec=" + minutesec +
                ", ip='" + ip + '\'' +
                ", regionid='" + regionid + '\'' +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", ispid='" + ispid + '\'' +
                ", reqid='" + reqid + '\'' +
                ", supplierid='" + supplierid + '\'' +
                ", publisherid='" + publisherid + '\'' +
                ", impid='" + impid + '\'' +
                ", cpid='" + cpid + '\'' +
                ", adid='" + adid + '\'' +
                ", cost_type='" + cost_type + '\'' +
                ", crid='" + crid + '\'' +
                ", uid='" + uid + '\'' +
                ", event_type='" + event_type + '\'' +
                ", price=" + price +
                ", bid_price=" + bid_price +
                ", ua='" + ua + '\'' +
                ", ref_url='" + ref_url + '\'' +
                ", risk=" + risk +
                ", risk_reason='" + risk_reason + '\'' +
                ", checksum='" + checksum + '\'' +
                ", alg_tag='" + alg_tag + '\'' +
                ", dmp_tag='" + dmp_tag + '\'' +
                ", ext=" + ext +
                ", host='" + host + '\'' +
                '}';
    }
}
