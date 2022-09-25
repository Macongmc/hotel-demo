package cn.itcast.hotel;

import cn.itcast.hotel.constants.HotelConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class HotelIndexTest {

    private RestHighLevelClient restClient;
    @BeforeEach
    public void setup(){
        restClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.200.141:9200")
        ));
    }
    @Test
    public  void  selectEs(){
        System.out.println(restClient);
    }

    /**
     * 创建索引
     * @throws IOException
     */
    @Test
    public void createIndex() throws IOException {
        //1.创建Request
        CreateIndexRequest request = new CreateIndexRequest("hotel");
        //2.准备DSL语句
        request.source(HotelConstants.MAPPING_TEMPLATE, XContentType.JSON);
        //3.发送请求
        restClient.indices().create(request, RequestOptions.DEFAULT);
    }

    /**
     * 删除索引
     * @throws IOException
     */
    @Test
    public void deleteIndex() throws IOException {
        //1.创建Request
        DeleteIndexRequest request = new DeleteIndexRequest("hotel");
        //2.发送请求
        restClient.indices().delete(request,RequestOptions.DEFAULT);
    }

    /**
     * 检测索引是否存在
     * @throws IOException
     */
    @Test
    public void existIndex() throws IOException {
        //1.创建Request
        GetIndexRequest request = new GetIndexRequest("hotel");
        //2.发送请求
        boolean exists = restClient.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    @AfterEach
    public void closeEs() throws IOException {
        this.restClient.close();
    }
}
