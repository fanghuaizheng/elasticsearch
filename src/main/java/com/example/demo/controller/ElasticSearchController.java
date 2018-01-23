package com.example.demo.controller;

import com.alibaba.fastjson.JSONObject;
import com.example.demo.entity.Employee;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by woni on 18/1/21.
 */
@RestController
@RequestMapping("es")
public class ElasticSearchController {

    private static Logger logger = LoggerFactory.getLogger(ElasticSearchController.class);

    @RequestMapping("getBySearch")
    public Object getResult(@RequestParam(value = "key",required = false,defaultValue = "11")String key){
        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

//        MatchQueryBuilder matchbuilder;
//        matchbuilder = QueryBuilders.matchQuery("first_name", "John");

        QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("first_name", "John")
                .fuzziness(Fuzziness.AUTO)
                .prefixLength(3)
                .maxExpansions(10);

        searchSourceBuilder.query(QueryBuilders.matchAllQuery());

//        searchSourceBuilder.from(0);
//        searchSourceBuilder.size(10);
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
//
//        searchSourceBuilder.aggregation(AggregationBuilders.terms("top_10_states").field("state").size(10));

//        SearchRequest searchRequest = new SearchRequest("megacorp");
//        searchRequest.types("employee");

        SearchRequest searchRequest = new SearchRequest();
//        searchRequest.types();

//        searchRequest.indices("social-*");

        searchRequest.source(searchSourceBuilder);


        SearchResponse searchResponse = null;
        try {
            searchResponse = client.search(searchRequest);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return searchRequest;
        }
    }

    /**
     * 通过查询 索引-类型-id，获取到搜索结果
     * @param index
     * @param type
     * @param id
     * @return
     */
    @RequestMapping("getValueById")
    public Object getValue(@RequestParam("index")String index,
                           @RequestParam("type")String type,
                           @RequestParam("id")String id){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));

        GetRequest request = new GetRequest(index, type, id);

        Map<String, Object> sourceAsMap = null;
        try {
            GetResponse getResponse = client.get(request);

            String getResponseindex = getResponse.getIndex();
            String getResponsetype = getResponse.getType();
            String getResponseid = getResponse.getId();
            if (getResponse.isExists()) {
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString();
                sourceAsMap = getResponse.getSourceAsMap();

            } else {//document not find
            }
        } catch (ElasticsearchException e) {
            if (e.status() == RestStatus.NOT_FOUND) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return sourceAsMap;
        }


    }

    /**
     * 向指定的 索引-类型-{id}里面增加数据
     * @return
     */
    @RequestMapping("addIndexValue")
    public Object addIndexValue(){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));

        String index = "megacorp";
        String type = "employee";
//        String id = "4";

        IndexRequest request = new IndexRequest(index,type);

        Employee employee = new Employee();

        employee.setFirst_name("hyzhang1");

        employee.setLast_name("xiaolajiao1");

        employee.setAbout("I love to go rock climbing");

        employee.setAge(26);

        employee.setInterests(new String[]{"Thinking","Skiing"});

        employee.setCreateTime(new Date());

        String string = JSONObject.toJSONString(employee);

        request.source(string, XContentType.JSON);

        try {
            IndexResponse indexResponse = client.index(request);

            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {

                logger.info("创建成功");

            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {

                logger.info("更新成功");

            }
            ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

                logger.info("集群分片更新不对");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.info("集群失败的原因：\t"+reason);
                }
            }

        } catch(ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return null;
        }


    }

    @RequestMapping("deleteById")
    public void deleteById(@RequestParam(value = "id",required = false,defaultValue = "1")String id){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));

        String index = "megacorp";
        String type = "employee";

        try {
            DeleteRequest request = new DeleteRequest(index, type, id);
            DeleteResponse deleteResponse = client.delete(request);
            if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {//没有找到
                logger.info("在指定的索引：\t"+index+"指定的类型:\t"+type+"指定的id:\t"+id+"没有找到删除的数据");

            }

            ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {
                logger.info("集群里面操作失败");
            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                    logger.info("删除失败的原因：\t"+reason);
                }
            }

        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    /**
     * 更新操作
     * 指定的索引-类型-id，指定的field修改值
     * @return
     */
    @RequestMapping("updateById")
    public Object updateById(){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));

        String index = "megacorp";
        String type = "employee";
        String id = "4";

        UpdateRequest request = new UpdateRequest(index,type,id)
                .doc("first_name", "hyzhang1");
        Map<String, Object> sourceAsMap = null;
        try {
            UpdateResponse updateResponse = client.update(request);

            GetResult result = updateResponse.getGetResult();
            if (result.isExists()) {
                sourceAsMap   = result.sourceAsMap();
            } else {//更新操作的数据不存在

                logger.info("更新的数据不存在");
            }


            //集群操作
            ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
            if (shardInfo.getTotal() != shardInfo.getSuccessful()) {

            }
            if (shardInfo.getFailed() > 0) {
                for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                    String reason = failure.reason();
                }
            }

        } catch(ElasticsearchException e) {
            if (e.status() == RestStatus.CONFLICT) {

            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            return sourceAsMap;
        }
    }


    /**
     * 批量操作啊，在一次请求里面包含多个请求操作
     */
    @RequestMapping("bulkOPeration")
    public void bulkOperation(){

        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(
                new HttpHost("localhost", 9200, "http")));

        String index = "megacorp";
        String type = "employee";

        Employee employee = new Employee();

        employee.setFirst_name("fhz");

        employee.setLast_name("xiaofang");

        employee.setAbout("I love to go rock");

        employee.setAge(26);

        employee.setInterests(new String[]{"Thinking","Skiing"});

        employee.setCreateTime(new Date());


        BulkRequest request = new BulkRequest();
        request.add(new DeleteRequest(index, type, "3"));
        request.add(new UpdateRequest(index, type, "2")
                .doc(XContentType.JSON,"first_name", "Jane_1"));
        request.add(new IndexRequest(index, type, "5")
                .source(JSONObject.toJSONString(employee),XContentType.JSON));

        try {
            BulkResponse bulkResponse = client.bulk(request);

            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();

                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    logger.info("创建成功");

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
                    logger.info("更新成功");

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                    logger.info("删除成功");
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }


    }

}
