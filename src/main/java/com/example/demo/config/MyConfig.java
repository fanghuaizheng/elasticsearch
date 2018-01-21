//package com.example.demo.config;
//
//import org.elasticsearch.client.transport.TransportClient;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.transport.client.PreBuiltTransportClient;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//
//import java.net.InetAddress;
//import java.net.UnknownHostException;
//
///**
// * Created by woni on 18/1/21.
// */
//@Configuration
//public class MyConfig {
//
//    @Value("${elasticsearch.cluster.name}")
//    private String clusterName;
//
//    @Value("${elasticsearch.host}")
//    private String host;
//
//
//    @Bean
//    public TransportClient client() throws UnknownHostException {
//
//
//        // 设置集群名称
//        Settings settings = Settings.builder().put("cluster.name", clusterName)
//                .build();
//        TransportClient transportClient = new PreBuiltTransportClient(settings);
//        String[] nodes = host.split(",");
//        for (String node : nodes) {
//            if (node.length() > 0) {
//                String[] hostPort = node.split(":");
//                transportClient.addTransportAddress(
//                        new InetSocketTransportAddress(
//                                InetAddress.getByName(hostPort[0]),
//                                Integer.parseInt(hostPort[1])));
//            }
//        }
//        return transportClient;
//    }
//}
