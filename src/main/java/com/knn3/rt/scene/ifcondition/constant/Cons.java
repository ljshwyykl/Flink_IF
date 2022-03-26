package com.knn3.rt.scene.ifcondition.constant;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

/**
 * @Author apophis
 * @File Cons
 * @Time 2022/3/25 13:53
 * @Description 工程描述
 */
public class Cons {
    public static final ObjectMapper MAPPER = Optional.of(
            new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    // 属性为NULL 不序列化
                    .setSerializationInclusion(JsonInclude.Include.NON_NULL)
    ).get();

    public static final String DCP_TABLE = "S_Impossible_Finance";
    public final static String STATUS_INSERT = "INSERT INTO public.\"F_Status\"(id,address,dcp_table,uid) VALUES(?,?,?,?) ON CONFLICT DO NOTHING;";
    public final static String STATUS_DELETE = "DELETE FROM public.\"F_Status\" where dcp_table=? and address in(?);";
    public final static String FINANCE_INSERT = "INSERT INTO public.\"S_Impossible_Finance\"(id,chain_id,contract_id,token_symbol,token_name,address,campaign_id,campaign_name,block_number,if_fans_token_threshold,balance) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT (address) DO UPDATE SET balance = ?;";
    public final static String FINANCE_DELETE = "DELETE FROM public.\"S_Impossible_Finance\" where address in(?);";
}
