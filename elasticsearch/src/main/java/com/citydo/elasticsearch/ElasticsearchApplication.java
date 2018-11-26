package com.citydo.elasticsearch;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

//Springboot 默认支持2种技术和ES交互
//jest（默认bu生效）
//需要导入io.searchbox.client.JestClient
// SpringData Elasticsearch
//client 节点clusterNodes clusterName
//ElasticsearchTemplate 操作es
// 编写Elasticsearchrepository的子接口操作ES
//2、SpringData ElasticSearch【ES版本有可能不合适】
//版本适配说明：https://github.com/spring-projects/spring-data-elasticsearch
//如果版本不适配：2.4.6
//1）、升级SpringBoot版本
//2）、安装对应版本的ES


/***
 * 采用客户端方式进行9300端口进行查询
 *通过创建索引进行查询
 *
 */

@SpringBootApplication
@RestController
public class ElasticsearchApplication {

    public static final Logger logger = LoggerFactory.getLogger(ElasticsearchApplication.class);

    @Autowired
    private TransportClient client;

    public static final String PEOPLE_INDEX = "people";
    public static final String PEOPLE_TYPE_MAN = "man";

    @GetMapping("/")
    public String index(){
        return "index";
    }

    @GetMapping("/get/people/man")
    @ResponseBody
    public ResponseEntity get(@RequestParam(name = "id",defaultValue = "") String id){
        if(id.isEmpty()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        GetResponse result = this.client.prepareGet("people","man",id)
                .get();
        if(!result.isExists()){
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }
        return new ResponseEntity(result.getSource(),HttpStatus.OK);
    }


    // 增加接口
    @PostMapping("/add/people/man")
    public ResponseEntity add(
            @RequestParam(name = "name") String name,
            @RequestParam(name = "country") String country,
            @RequestParam(name = "age") int age,
            @RequestParam(name = "date")
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date date) {

        try {
            XContentBuilder content = XContentFactory.jsonBuilder().startObject()
                    .field("name", name)
                    .field("country", country)
                    .field("age", age)
                    .field("date", date.getTime())
                    .endObject();

            IndexResponse response = client.prepareIndex(PEOPLE_INDEX, PEOPLE_TYPE_MAN)
                    .setSource(content)
                    .get();

            return new ResponseEntity(response.getId(), HttpStatus.OK);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    // 删除接口
    @DeleteMapping("/delete/people/man")
    @ResponseBody
    public ResponseEntity delete(@RequestParam(name = "id") String id) {
        DeleteResponse response = client.prepareDelete(PEOPLE_INDEX, PEOPLE_TYPE_MAN, id).get();
        return new ResponseEntity(response.getResult().toString(), HttpStatus.OK);
    }


    // 更新接口
    @PutMapping("/update/people/man")
    @ResponseBody
    public ResponseEntity update(
            @RequestParam(name = "id") String id,
            @RequestParam(name = "name", required = false) String name,
            @RequestParam(name = "country", required = false) String country,
            @RequestParam(name = "age", required = false) Integer age,
            @RequestParam(name = "date", required = false)
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss") Date date) {

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            if (name != null) {
                builder.field("name", name);
            }
            if (country != null) {
                builder.field("country", country);
            }
            if (age != null) {
                builder.field("age", age);
            }
            if (date != null) {
                builder.field("date", date.getTime());
            }
            builder.endObject();

            UpdateRequest update = new UpdateRequest(PEOPLE_INDEX, PEOPLE_TYPE_MAN, id);
            update.doc(builder);
            UpdateResponse response = client.update(update).get();
            return new ResponseEntity(response.getResult().toString(), HttpStatus.OK);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }

    }

    // 复合查询
    @PostMapping("query/people/man")
    @ResponseBody
    public ResponseEntity query(
            @RequestParam(name = "name", required = false) String name,
            @RequestParam(name = "country", required = false) String country,
            @RequestParam(name = "gt_age", defaultValue = "0") int gtAge,
            @RequestParam(name = "lt_age", required = false) Integer ltAge) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
        if (country != null) {
            boolQuery.must(QueryBuilders.matchQuery("country", country));
        }
        if (name != null) {
            boolQuery.must(QueryBuilders.matchQuery("title", name));
        }
        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("age").from(gtAge);
        if (ltAge != null && ltAge > 0) {
            rangeQuery.to(ltAge);
        }
        boolQuery.filter(rangeQuery);
        SearchRequestBuilder builder = client.prepareSearch(PEOPLE_INDEX).setTypes(PEOPLE_TYPE_MAN)
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);
        logger.debug(builder.toString());
        SearchResponse response = builder.get();
        List result = new ArrayList<Map<String, Object>>();
        for (SearchHit hit : response.getHits()) {
            result.add(hit.getSourceAsMap());
        }
        return new ResponseEntity(result, HttpStatus.OK);

    }


    public static void main(String[] args) {
        SpringApplication.run(ElasticsearchApplication.class, args);
    }
}
