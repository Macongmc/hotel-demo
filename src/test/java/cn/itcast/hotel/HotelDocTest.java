package cn.itcast.hotel;

import cn.itcast.hotel.pojo.Hotel;
import cn.itcast.hotel.pojo.HotelDoc;
import cn.itcast.hotel.service.IHotelService;
import com.alibaba.fastjson.JSON;
import com.baomidou.mybatisplus.extension.api.R;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = HotelDemoApplication.class)
public class HotelDocTest {
    @Autowired
    private IHotelService iHotelService;
    private RestHighLevelClient restClient;
    @BeforeEach
    public void setup(){
        restClient = new RestHighLevelClient(RestClient.builder(
                HttpHost.create("http://192.168.200.141:9200")
        ));
    }
    @AfterEach
    public void closeEs() throws IOException {
        this.restClient.close();
    }

    /**
     * 新增文档
     * @throws IOException
     */
    @Test
    public void addDoc() throws IOException {
        //根据id查询
        Hotel hotel = iHotelService.getById(36934l);
        //转换为文档类型
        HotelDoc hotelDoc = new HotelDoc(hotel);
        //1.准备Request
        IndexRequest request = new IndexRequest("hotel").id(hotel.getId().toString());
        //2.准备json文档
        request.source(JSON.toJSONString(hotelDoc), XContentType.JSON);
        //3.发送请求
        restClient.index(request, RequestOptions.DEFAULT);
    }

    /**
     * 根据id查询doc
     * @throws IOException
     */
    @Test
    public void getDocById() throws IOException {
        //1.创建请求
        GetRequest request = new GetRequest("hotel","36934");
        //2.发送请求
        GetResponse getResponse = restClient.get(request, RequestOptions.DEFAULT);
        //3.解析
        String json = getResponse.getSourceAsString();

        HotelDoc hotelDoc = JSON.parseObject(json, HotelDoc.class);
        System.out.println(hotelDoc);
    }

    /**
     * 修改局部字段
     * @throws IOException
     */
    @Test
    public void updateDoc() throws IOException {
        //1.创建请求
        UpdateRequest request =new UpdateRequest("hotel","36934");
        request.doc(
                "price", "952",
                "starName", "四钻"
        );
        //2.发送请求
        restClient.update(request,RequestOptions.DEFAULT);
    }

    /**
     * 删除Doc
     * @throws IOException
     */
    @Test
    public void deleteDoc() throws IOException {
        //1.创建请求
        DeleteRequest request = new DeleteRequest("hotel","36934");
        //2.发送请求
        restClient.delete(request,RequestOptions.DEFAULT);
    }

    /**
     * 批量处理
     * @throws IOException
     */
    @Test
    public void Bulk() throws IOException {
        // 批量查询酒店数据
        List<Hotel> hotels = iHotelService.list();

        // 1.创建Request
        BulkRequest request = new BulkRequest();
        // 2.准备参数，添加多个新增的Request
        for (Hotel hotel : hotels) {
            // 2.1.转换为文档类型HotelDoc
            HotelDoc hotelDoc = new HotelDoc(hotel);
            // 2.2.创建新增文档的Request对象
            request.add(new IndexRequest("hotel")
                    .id(hotelDoc.getId().toString())
                    .source(JSON.toJSONString(hotelDoc), XContentType.JSON));
        }
        // 3.发送请求
        restClient.bulk(request, RequestOptions.DEFAULT);
    }
}
