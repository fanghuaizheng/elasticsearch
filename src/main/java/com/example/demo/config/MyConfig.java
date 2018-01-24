package com.example.demo.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

/**
 * Created by woni on 18/1/21.
 */
@Configuration
public class MyConfig {

    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    @Value("${elasticsearch.host}")
    private String host;

    @Value("${elasticsearch.port:9200}")
    private Integer port;

    @Value("${elasticsearch.search.test.index}")
    public String index;

    @Value("${elasticsearch.search.test.type}")
    public String type;



}
