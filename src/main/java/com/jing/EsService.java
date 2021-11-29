package com.jing;

import com.jing.utils.CollectionUtil;
import com.jing.utils.StringUtil;
import com.jing.utils.ThrowableUtil;
import lombok.extern.slf4j.Slf4j;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;


import java.io.File;
import java.util.*;

@Slf4j
public class EsService {

    private String hosts;

    private Integer httpPort;

    private RestHighLevelClient client;


    public void EsService(String hosts,Integer httpPort) {
        if(hosts==null||"".equals(hosts.trim())){
            log.warn("es hosts is null");

        }
        if(httpPort<=0){
            log.warn("es http port is <=0");

        }
        log.info("start connect to es hosts: "+hosts+ " with http port: "+ httpPort);
        String[] hostsArr = hosts.split(",");
        HttpHost[] httpHosts = new HttpHost[hostsArr.length];
        for(int i = 0 ; i< hostsArr.length;i++){
            httpHosts[i] = new HttpHost(hostsArr[i], httpPort, "http");
        }

        this.client = new RestHighLevelClient(RestClient.builder(httpHosts));
        this.hosts = hosts;
        this.httpPort = httpPort;

    }

    public List<String> analysisTextByIkSmart(String text) {
        return  analysisText(text,"ik_smart");
    }

    public List<String> analysisTextByIkMaxWord(String text) {
        return  analysisText(text,"ik_max_word");
    }

    public List<String> analysisText(String text,String analyzer) {
        List<String> words = new ArrayList<>();

        if(StringUtil.isEmpty(text)){
            return  words;
        }
        if(StringUtil.isEmpty(analyzer)){
            analyzer = "ik_max_word";
        }
        try {
            AnalyzeRequest analyzeRequest = AnalyzeRequest.withGlobalAnalyzer(analyzer,text);
            AnalyzeResponse analyzeResponse = client.indices().analyze(analyzeRequest, RequestOptions.DEFAULT);
            List<AnalyzeResponse.AnalyzeToken> listAnalysis = analyzeResponse.getTokens(); // 获取所有分词的内容
            if (CollectionUtil.isEmpty(listAnalysis)) {
                words.add(text);
                return words;
            }
            listAnalysis.forEach(ikToken -> { words.add(ikToken.getTerm()); });
        } catch (Exception e) {
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return words;
    }

    public boolean deleteIndex(String index){
        boolean boolRes = false;
        try {
            DeleteIndexRequest request = new DeleteIndexRequest(index);
            AcknowledgedResponse deleteIndexResponse = this.client.indices().delete(request, RequestOptions.DEFAULT);
            boolean acknowledged = deleteIndexResponse.isAcknowledged();
            if(acknowledged){
                log.info("es delete index "+ index+" successful");
                boolRes = true;
            }
        } catch (ElasticsearchException exception) {
            if (exception.status() == RestStatus.NOT_FOUND) {
                log.warn("es delete index "+ index+" is not exists");
            }
        } catch (Exception e){
            log.info("es delete index "+ index+" failed");
        }
        return boolRes;
    }

    public boolean createIndexWithJsonFile(String index,String filePath){
        boolean boolRes = false;
        try {
            //Scanner sc = new Scanner(new File(System.getProperty("user.dir")+"/config/es-knowledge.json"));
            Scanner sc = new Scanner(new File(filePath));
            StringBuilder sb = new StringBuilder();
            while (sc.hasNext()) {
                sb.append(sc.nextLine()).append("\n");
            }
            log.info("es create index:\n"+sb.toString());

            CreateIndexRequest request = new CreateIndexRequest(index);
            request.source(sb.toString(), XContentType.JSON);
            CreateIndexResponse createIndexResponse = this.client.indices().create(request, RequestOptions.DEFAULT);
            boolean acknowledged = createIndexResponse.isAcknowledged();
            boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
            createIndexResponse.index();
            if(acknowledged&&shardsAcknowledged){
                log.info("es create index "+ index+" successful");
                boolRes = true;
            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
            log.info("es create index "+ index+" failed");
        }
        return  boolRes;
    }

    public boolean insert(String index, String type,String id, Map<String,Object> map,boolean replaceCheck){
        boolean result = false;
        //true:allow to replace the exits id, false:not allow to replace the exits id
        if(replaceCheck==false){
            // if id exists,not allow to insert
            if(checkExistsId(index,type,id)){
                log.warn("es not allow to insert, because index="+index+",type="+type+",id="+id + " already exists");
                return  false;
            }
        }
        try{
            IndexRequest request = new IndexRequest(index, type, id).source(map);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            IndexResponse response = this.client.index(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.CREATED) {
                result = true;
            }

        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public boolean delete(String index, String type,String id){
        boolean result = false;
        try{
            DeleteRequest request = new DeleteRequest(index, type, id);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            DeleteResponse response = client.delete(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.DELETED) {
                result = true;
            }
        }catch (Exception e){
            log.error("delete es index="+index+",type="+type+",id="+id + " failed:\n"+ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public boolean deleteBulk(String index, String type, List<String> list){
        if(list==null||list.size()==0){
            return false;
        }else {
            return deleteBulk(index, type, list.toArray(new String[0]));
        }
    }

    public boolean deleteBulk(String index, String type,String[] ids){
        boolean result = false;
        try{
            if(ids==null||ids.length==0){
                return false;
            }

            BulkRequest request = new BulkRequest();
            for(int i=0;i<ids.length;i++){
                request.add(new DeleteRequest(index, type, ids[i]));
            }
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            BulkResponse response = client.bulk(request, RequestOptions.DEFAULT);
            if (response.hasFailures()) {
                result = false;
            }else {
                result = true;
            }
        }catch (Exception e){
            log.error("delete es index="+index+",type="+type+",ids="+ Arrays.toString(ids) + " failed:\n"+ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public boolean update(String index, String type,String id, Map<String,Object> map){
        boolean result = false;
        try{
            UpdateRequest request = new UpdateRequest(index, type, id).doc(map);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            UpdateResponse response = client.update(request, RequestOptions.DEFAULT);
            if (response.getResult() == DocWriteResponse.Result.UPDATED) {
                result = true;
            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public boolean checkExistsId(String index, String type,String id){
        boolean result = false;
        try {
            GetRequest getRequest = new GetRequest(index, type, id);
            getRequest.fetchSourceContext(new FetchSourceContext(false));
            getRequest.storedFields("_none_");
            result = client.exists(getRequest, RequestOptions.DEFAULT);
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public Map<String,Object> getAllFieldValue(String index, String type,String id){
        Map<String, Object> map = new HashMap<>();
        try {
            GetRequest request = new GetRequest(index, type, id);
            request.fetchSourceContext(FetchSourceContext.FETCH_SOURCE);

            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                map = response.getSourceAsMap();
            } else {

            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return map;
    }

    public Map<String,Object> getSelectFieldValue(String index, String type,String id,String[] fetchArr){
        Map<String, Object> map = new HashMap<>();
        try {
            GetRequest request = new GetRequest(index, type, id);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            String[] includeFields = fetchArr;
            String[] excludeFields = Strings.EMPTY_ARRAY;
            sourceBuilder.fetchSource(includeFields, excludeFields);
            GetResponse response = client.get(request, RequestOptions.DEFAULT);
            if (response.isExists()) {
                map = response.getSourceAsMap();
            } else {

            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return map;
    }

    public String getFieldValue(String index, String type,String id,String fieldName) {
        String result = null;
        try{
            GetRequest request = new GetRequest(index,type,id);
            String[] includes = {fieldName};
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);
            request.fetchSourceContext(fetchSourceContext);
            GetResponse getResponse = client.get(request, RequestOptions.DEFAULT);
            if (getResponse.isExists()) {
                Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
                result = (String) sourceAsMap.get(fieldName);

            } else {

            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
       return result;
    }


    public  MultiGetItemResponse[]  getMulti(String index, String type,List<String> idList,String[] fetchArr){
        MultiGetItemResponse[] result=null;

        try {
            String[] includes = fetchArr;
            String[] excludes = Strings.EMPTY_ARRAY;
            FetchSourceContext fetchSourceContext =
                    new FetchSourceContext(true, includes, excludes);

            MultiGetRequest request = new MultiGetRequest();
            if(idList!=null&&idList.size()>0){
                for(String id:idList) {
                    request.add(new MultiGetRequest.Item(index, type,id).fetchSourceContext(fetchSourceContext));
                }
            }
            MultiGetResponse response = client.mget(request, RequestOptions.DEFAULT);
            result =response.getResponses();
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return result;
    }

    public BoolQueryBuilder constructBoolQuery(Map<String,String> mustQueryMap,Map<String,String>  shouldQueryMap,
                                               Map<String,String> startQueryMap,Map<String,String> endQueryMap){
        boolean mustCheck = false;
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        if(mustQueryMap!=null&&mustQueryMap.size()>0){
            mustCheck=true;
            constructMustMatchQuery(queryBuilder,mustQueryMap);
        }
        if(startQueryMap!=null&&endQueryMap!=null&&startQueryMap.size()>0&&startQueryMap.size()==endQueryMap.size()){
            mustCheck=true;
            for(String key:startQueryMap.keySet()){
                constructRangeQuery(queryBuilder,key,startQueryMap.get(key),endQueryMap.get(key));
            }
        }
        if(mustCheck){
            BoolQueryBuilder queryBuilder1 = QueryBuilders.boolQuery();
            constructOrQuery(queryBuilder1,shouldQueryMap);
            queryBuilder.must(queryBuilder1);
        }else {
            constructShouldMatchQuery(queryBuilder,shouldQueryMap);
        }
        return queryBuilder;
    }

    public void constructMustMatchQuery(BoolQueryBuilder queryBuilder, Map<String,String> map) {
        if(map==null||map.size()==0){
            log.info("must map is empty");
            return;
        }else {
            for(String key:map.keySet()) {
                constructMustMatchQuery(queryBuilder,key,map.get(key));
            }
        }
    }

    public void constructMustMatchQuery(BoolQueryBuilder queryBuilder, String field, String value) {
        if (value != null && !"".equals(value)) {
            queryBuilder.must(QueryBuilders.matchQuery(field, value));
        }
    }

    public void constructRangeQuery(BoolQueryBuilder queryBuilder, String field, String startvalue, String endValue) {
        if (startvalue != null && !"".equals(startvalue)) {
            if (endValue != null && !"".equals(endValue)) {
                queryBuilder.must(QueryBuilders.rangeQuery(field).gte(startvalue).lte(endValue));
            } else {
                queryBuilder.must(QueryBuilders.rangeQuery(field).gte(startvalue));
            }
        } else if (endValue != null && !"".equals(endValue)) {
            queryBuilder.must(QueryBuilders.rangeQuery(field).lte(endValue));
        }
    }

    public void constructShouldMatchQuery(BoolQueryBuilder queryBuilder, Map<String,String> map) {
        if(map==null||map.size()==0){
            log.info("should map is empty");
            return;
        }else {
            for(String key:map.keySet()) {
                constructShouldMatchQuery(queryBuilder,key,map.get(key));
            }
        }
    }

    public void constructShouldMatchQuery(BoolQueryBuilder queryBuilder, String field, String value) {
        if (value != null && !"".equals(value)) {
            queryBuilder.should(QueryBuilders.matchQuery(field, value));
        }
    }

    public void constructOrQuery(BoolQueryBuilder queryBuilder, Map<String,String> map) {
        if(map==null||map.size()==0){
            log.info("or map is empty");
            return;
        }else {
            for(String key:map.keySet()) {
                constructOrQuery(queryBuilder,key,map.get(key));
            }
        }
    }

    public void constructOrQuery(BoolQueryBuilder queryBuilder, String field, String value){
        if (value != null && !"".equals(value)) {
            queryBuilder.should(QueryBuilders.boolQuery().filter(QueryBuilders.matchQuery(field,value)));
        }
    }

    public List<String> searchIds(String index,String type,Map<String,String> mustQueryMap ,Map<String,String> shouldQueryMap,
                                  Map<String,String> startQueryMap,Map<String,String> endQueryMap, String sortField){
        List<String> ids = null;
        try {
            BoolQueryBuilder queryBuilder = constructBoolQuery(mustQueryMap,shouldQueryMap,startQueryMap,endQueryMap);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(queryBuilder);
            if (sortField != null && !"".equals(sortField.trim())) {
                sourceBuilder.fetchSource(false)
                        .sort(new FieldSortBuilder(sortField).order(SortOrder.DESC));
            } else {
                sourceBuilder.fetchSource(false);
            }
            sourceBuilder.size(10000);

            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(type);
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            SearchHits hits = response.getHits();
            long sum = hits.getTotalHits().value;
            ids =new ArrayList<String>((int) sum);

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                //log.info("es中查到的uuid:"+hit.getId());
                ids.add(hit.getId());
            }
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return ids;
    }


    public SearchHits searchMustAndShould(String index,String type,Map<String,String> mustQueryMap ,Map<String,String> shouldQueryMap,String[] fetchArr){
        return search(index,type,shouldQueryMap,mustQueryMap,null,null,fetchArr,null);
    }

    public SearchHits searchShould(String index,String type,Map<String,String> shouldQueryMap,String[] fetchArr){
        return search(index,type,shouldQueryMap,null,null,null,fetchArr,null);
    }

    public SearchHits searchShould(String index,String type,Map<String,String> shouldQueryMap,String[] fetchArr,String sortField) {
          return search(index,type,shouldQueryMap,null,null,null,fetchArr,sortField);
    }

    public SearchHits searchMust(String index,String type,Map<String,String> mustQueryMap,String[] fetchArr) {
        return search(index,type,null,mustQueryMap,null,null,fetchArr,null);
    }
    public SearchHits searchMustWithSort(String index,String type,Map<String,String> mustQueryMap,String[] fetchArr,String sortField) {
        return search(index,type,null,mustQueryMap,null,null,fetchArr,sortField);
    }

    public SearchHits search(String index,String type,Map<String,String> mustQueryMap,Map<String,String>  shouldQueryMap,
                             Map<String,String> startQueryMap,Map<String,String> endQueryMap,
                             String[] fetchArr,String sortField){
        SearchHits hits = null;
        try {
            boolean mustCheck = false;
            BoolQueryBuilder queryBuilder = constructBoolQuery(mustQueryMap,shouldQueryMap,startQueryMap,endQueryMap);

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
            sourceBuilder.query(queryBuilder);
            sourceBuilder.size(10000);

            String[] includeFields = fetchArr;
            String[] excludeFields = Strings.EMPTY_ARRAY;

            if (sortField != null && !"".equals(sortField.trim())) {
                sourceBuilder.fetchSource(includeFields, excludeFields)
                        .sort(new FieldSortBuilder(sortField).order(SortOrder.DESC));
            } else {
                sourceBuilder.fetchSource(includeFields, excludeFields);
            }

            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(type);
            searchRequest.source(sourceBuilder);

            SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
            hits = response.getHits();
        }catch (Exception e){
            log.error(ThrowableUtil.getExceptionString(e));
        }
        return hits;
    }




    public SearchHits searchWithPage(String index,String type,Map<String,String> mustQueryMap,Map<String,String>  shouldQueryMap,
                                     Map<String,String> startQueryMap,Map<String,String> endQueryMap,
                                     String[] fetchArr,int pageNum,int pageSize,String sortField){
        SearchHits hits = null;
       try {
           BoolQueryBuilder queryBuilder = constructBoolQuery(mustQueryMap,shouldQueryMap,startQueryMap,endQueryMap);

           SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
           sourceBuilder.query(queryBuilder);
           sourceBuilder.size(10000);

           String[] includeFields = fetchArr;
           String[] excludeFields = Strings.EMPTY_ARRAY;

           if (sortField != null && !"".equals(sortField.trim())) {
               sourceBuilder.fetchSource(includeFields, excludeFields).from(pageNum * pageSize - pageSize).size(pageSize)
                       .sort(new FieldSortBuilder(sortField).order(SortOrder.DESC));
           } else {
               sourceBuilder.fetchSource(includeFields, excludeFields).from(pageNum * pageSize - pageSize).size(pageSize);
           }

           SearchRequest searchRequest = new SearchRequest(index);
           searchRequest.types(type);
           searchRequest.source(sourceBuilder);

           SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
           hits = response.getHits();
       }catch (Exception e){
           log.error(ThrowableUtil.getExceptionString(e));
       }
       return hits;
    }

}
