package com.jjt.flink.demo.schema;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;


public class BidRequest extends AbstractSchema<BidRequest> {
    public Timestamp ts;
    public Long date;
    public Long hour;
    public Long minutesec;
    public String ip;
    public String regionid;
    public String country;
    public String province;
    public String city;
    public String ispid;
    public String supplierid;
    public String reqid;
    public String impid;
    public Long ad_format;
    public String publisherid;
    public String publisher_domain;
    public String tagid;
    public Long bid_floor;
    public Long auction_type;
    public String page_url;
    public String ref_url;
    public String bundle;
    public String[] app_category;
    public String content_title;
    public String[] content_category;
    public String[] content_keywords;
    public Long device_type;
    public String ua;
    public String make	;
    public String model;
    public String os;
    public String osv;
    public String flashver;
    public String carrier;
    public Long connection_type;
    public String idfa	;
    public String didmd5;
    public String dpidmd5;
    public String cookieid;
    public Long yob;
    public Long gender;
    public Float lat;
    public Float lon;
    public String[] formats;
    public String[] pids;
    public Long risk;
    public String risk_reason;
    public Map ext;
    public Map features;
    public String[] user_group;
    public String host;
    public String[] dealids;
//    public String idfa_md5;
//    public String uid;
//    public Timestamp proc_time;

//    @Override
//    public BidRequest deserialize(byte[] bytes) throws IOException {
//        BidRequest object = super.deserialize(bytes);
//        object.ts = new Timestamp(System.currentTimeMillis());
//        return object;
//    }


    @Override
    public String toString() {
        return "BidRequest{" +
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
                ", supplierid='" + supplierid + '\'' +
                ", reqid='" + reqid + '\'' +
                ", impid='" + impid + '\'' +
                ", ad_format=" + ad_format +
                ", publisherid='" + publisherid + '\'' +
                ", publisher_domain='" + publisher_domain + '\'' +
                ", tagid='" + tagid + '\'' +
                ", bid_floor=" + bid_floor +
                ", auction_type=" + auction_type +
                ", page_url='" + page_url + '\'' +
                ", ref_url='" + ref_url + '\'' +
                ", bundle='" + bundle + '\'' +
                ", app_category=" + Arrays.toString(app_category) +
                ", content_title='" + content_title + '\'' +
                ", content_category=" + Arrays.toString(content_category) +
                ", content_keywords=" + Arrays.toString(content_keywords) +
                ", device_type=" + device_type +
                ", ua='" + ua + '\'' +
                ", make='" + make + '\'' +
                ", model='" + model + '\'' +
                ", os='" + os + '\'' +
                ", osv='" + osv + '\'' +
                ", flashver='" + flashver + '\'' +
                ", carrier='" + carrier + '\'' +
                ", connection_type=" + connection_type +
                ", idfa='" + idfa + '\'' +
                ", didmd5='" + didmd5 + '\'' +
                ", dpidmd5='" + dpidmd5 + '\'' +
                ", cookieid='" + cookieid + '\'' +
                ", yob=" + yob +
                ", gender=" + gender +
                ", lat=" + lat +
                ", lon=" + lon +
                ", formats=" + Arrays.toString(formats) +
                ", pids=" + Arrays.toString(pids) +
                ", risk=" + risk +
                ", risk_reason='" + risk_reason + '\'' +
                ", ext=" + ext +
                ", features=" + features +
                ", user_group=" + Arrays.toString(user_group) +
                ", host='" + host + '\'' +
                ", dealids=" + Arrays.toString(dealids) +
//                ", idfa_md5='" + idfa_md5 + '\'' +
//                ", uid='" + uid + '\'' +
                '}';
    }
}
