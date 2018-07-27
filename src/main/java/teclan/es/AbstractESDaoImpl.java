package teclan.es;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import teclan.es.utils.IdGenerater;
import teclan.es.utils.PageInfoUtils;

public abstract class AbstractESDaoImpl {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractESDaoImpl.class);

	public abstract TransportClient getTransportClient();

	public abstract String getIndex();

	public abstract String getType();

	/**
	 * 添加文档
	 * 
	 * @param id
	 * @param document
	 * @return
	 */
	public boolean addDocument(String id, JSONObject document) {
		if (!document.containsKey("id")) {
			document.put("id", id);
		}

		getTransportClient().prepareIndex(getIndex(), getType(), id).setSource(document).setRefresh(true).execute()
				.actionGet();
		return true;
	}

	/**
	 * 添加多个文档
	 * 
	 * @param index
	 * @param type
	 * @param documents
	 * @return
	 */
	public boolean addDocuments(JSONArray documents) {

		for (int i = 0; i < documents.size(); i++) {
			JSONObject document = documents.getJSONObject(i);
			addDocument(IdGenerater.getNextId(), document);
		}
		return true;
	}

	/**
	 * 删除文档
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @return
	 */
	public boolean deleteDocument(String id) {
		DeleteResponse result = getTransportClient().prepareDelete().setRefresh(true).setIndex(getIndex())
				.setType(getType()).setId(id).execute().actionGet();

		boolean isfound = result.isFound();// 是否删除成功
		return isfound;
	}

	/**
	 * 修改文档
	 * 
	 * @param index
	 * @param type
	 * @param id
	 * @param namesAndValues 新的字段和值
	 * @return
	 */
	public boolean updateDocument( String id, JSONObject namesAndValues) {
		UpdateRequest updateRequest = new UpdateRequest(getIndex(), getType(), id);
		updateRequest.doc(namesAndValues);

		BulkRequestBuilder builder = getTransportClient().prepareBulk().setRefresh(true);
		builder.add(updateRequest);
		try {
			builder.execute().actionGet();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		}
		return true;
	}

	/**
	 * 统计文档数量
	 * 
	 * @param index
	 * @param type
	 * @param boolQuery
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public long count(String index, String type, BoolQueryBuilder boolQuery) {
		CountRequestBuilder countRequestBuilder = getTransportClient().prepareCount(index).setTypes(type)
				.setQuery(boolQuery);
		CountResponse countResponse = countRequestBuilder.execute().actionGet();

		return countResponse.getCount();
	}

	/**
	 * 获取文档列表信息
	 * 
	 * @param index
	 * @param type
	 * @param currentPage
	 * @param pageSize
	 * @param boolQuery
	 * @param sorts
	 * @return
	 */
	public JSONObject query(String index, String type, int currentPage, int pageSize, BoolQueryBuilder boolQuery,
			SortBuilder... sorts) {

		long total = count(index, type, boolQuery);
		int totalPages = PageInfoUtils.getTotalPages(total, pageSize);

		int offset = PageInfoUtils.getOffset(currentPage, pageSize, totalPages);

		SearchRequestBuilder searchRequestBuilder = getTransportClient().prepareSearch(index).setTypes(type)
				.setQuery(boolQuery).setFrom(offset).setSize(pageSize);

		for (SortBuilder sort : sorts) {
			searchRequestBuilder.addSort(sort);
		}
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		JSONArray array = new JSONArray();
		for (SearchHit hit : searchResponse.getHits()) {
			array.add(JSON.parseObject(hit.getSourceAsString()));
		}

		JSONObject pageInfo = PageInfoUtils.getPageInfo(total, totalPages, currentPage, pageSize);

		JSONObject result = new JSONObject();
		result.put("pageInfo", pageInfo);
		result.put("result", array);
		result.put("code", "200");
		result.put("message", "查询成功");

		return result;
	}

	public JSONObject query(String index, String type, BoolQueryBuilder boolQuery, SortBuilder... sorts) {

		SearchRequestBuilder searchRequestBuilder = getTransportClient().prepareSearch(index).setTypes(type)
				.setQuery(boolQuery);

		for (SortBuilder sort : sorts) {
			searchRequestBuilder.addSort(sort);
		}
		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		JSONArray array = new JSONArray();
		for (SearchHit hit : searchResponse.getHits()) {
			array.add(JSON.parseObject(hit.getSourceAsString()));
		}

		JSONObject result = new JSONObject();
		result.put("result", array);

		return result;
	}

	public JSONObject queryById(String index, String type, String id) {

		BoolQueryBuilder boolQuery = new BoolQueryBuilder();
		boolQuery.must(QueryBuilders.wildcardQuery("id", id));

		SearchRequestBuilder searchRequestBuilder = getTransportClient().prepareSearch(index).setTypes(type)
				.setQuery(boolQuery);

		SearchResponse searchResponse = searchRequestBuilder.execute().actionGet();

		JSONObject obj = new JSONObject();
		for (SearchHit hit : searchResponse.getHits()) {
			obj = JSON.parseObject(hit.getSourceAsString());
			break;
		}

		JSONObject result = new JSONObject();
		result.put("result", obj);

		return result;
	}

}
