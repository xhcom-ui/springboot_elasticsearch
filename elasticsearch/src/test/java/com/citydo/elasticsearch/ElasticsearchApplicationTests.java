package com.citydo.elasticsearch;

import com.citydo.elasticsearch.bean.Article;
import com.citydo.elasticsearch.bean.Book;
import com.citydo.elasticsearch.repository.BookRepository;
import io.searchbox.client.JestClient;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class ElasticsearchApplicationTests {

    @Autowired
    JestClient jestClient;

    @Autowired
    BookRepository bookRepository;




    @Test
    public  void esDate(){
        //此方法适用于2.X版本
        //不适应5.x版本
        Book book = new Book();
        book.setId(1);
        book.setAuthor("熊点点");
        book.setBookName("西游降魔");
        bookRepository.index(book);
        //查询
        /*for (Book book : bookRepository.findByBookNameLike("游")) {
            System.out.println(book);
        };*/
    }


    //利用java客户端进行创建索引
    @Test
    public  void esConfig(){
        //1.设置集群名称
        Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();
        //2.创建client
        TransportClient client = null;
        try {
            client = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName("192.168.252.144"), 9300));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        //4.创建索引
        CreateIndexResponse ciReponse=indicesAdminClient.prepareCreate("index").get();

        System.out.println(ciReponse.isAcknowledged());
    }



	
    @Test
    public void contextLoads() {
        //1.给ES中索引一个文档
        Article article = new Article();
        article.setId(1);
        article.setTitle("好问题");
        article.setAuthor("zhangsan");
        article.setContent(" hello hello 呵呵");

        //构建索引
        Index build = new Index.Builder(article).index("atguigu").type("news").build();

        try {
            jestClient.execute(build);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public  void search(){

        String json="{\n" +
                "    \"query\" : {\n" +
                "        \"match\" : {\n" +
                "            \"content\" : \"hello\"\n" +
                "        }\n" +
                "    }\n" +
                "}";
        //或者
        /*String a="hello";
        JSONObject jsons=new JSONObject();
        try {
            jsons.put("query","match"+a);
        } catch (JSONException e) {
            e.printStackTrace();
        }*/
        //构建搜索
        Search build = new Search.Builder(json).addIndex("atguigu").addType("news").build();

        //执行
        try {
            SearchResult execute = jestClient.execute(build);
            System.err.println(execute.getJsonString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
