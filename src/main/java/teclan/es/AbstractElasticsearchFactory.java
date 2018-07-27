package teclan.es;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import teclan.es.utils.FileUtils;

/**
 * 用于初始化ES索引和字典
 * 
 * @author dev
 *
 */
public abstract class AbstractElasticsearchFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(AbstractElasticsearchFactory.class);
	public static final MediaType mediaType = MediaType.parse("application/json; charset=utf-8");

	private OkHttpClient client = new OkHttpClient();
	private Class<?> cls = null;
	/**
	 * json文件的目录
	 */
	private String jsonDir = ".";

	/**
	 * 用于创建索引和初始化索引数据
	 * 
	 * 
	 * 默认json文件在classPath中的路径前缀为空，创建索引的json</br>
	 * 文件所在目录为 indexs/,创建默认数据的json文件所在目录为 datas/,</br>
	 * 所有的创建索引的json文件名规则为index-[索引名称]-[类型]，</br>
	 * 所有的创建数据的json文件名规则为data-[索引名称]-[类型]，</br>
	 * 内容必须是一个json数组，并且有不重复的id字段</br>
	 * 
	 * 创建索引的json文件内容示例（index-plan-plan.json）：</br>
	 * 
	 * {</br>
	 * "settings": {</br>
	 * "number_of_shards": 5,</br>
	 * "number_of_replicas": 0,</br>
	 * "max_result_window": 200000000</br>
	 * },</br>
	 * "mappings": {</br>
	 * "plan": {</br>
	 * "properties": {</br>
	 * "id": {</br>
	 * "type": "string",</br>
	 * "index": "not_analyzed"</br>
	 * },</br>
	 * "typeName": {</br>
	 * "type": "string",</br>
	 * "index": "not_analyzed"</br>
	 * }</br>
	 * }</br>
	 * }</br>
	 * }</br>
	 * }</br>
	 * 
	 * 创建索引数据的文件内容示例（data-plan-plan.json）：</br>
	 * 
	 * [{</br>
	 * "id":"1",</br>
	 * "typeName":"安保维稳"</br>
	 * },{</br>
	 * "id":"2",</br>
	 * "typeName":"应急处置"</br>
	 * } ]
	 * 
	 * 
	 * 
	 */
	public void init() {
		this.jsonDir = "";
		this.cls = getClass();
		createIndexIfNeed();
		createIndexDataIfNeed();
	}

	/**
	 * 用于创建索引和初始化索引数据
	 * 
	 * 
	 * @param jsonDir json文件在 classPath 中的路径，
	 * 
	 *                例如 jsonDir 为 当前路径，则创建索引的json文件所在目录</br>
	 *                为 indexs/,创建默认数据的json文件所在目录为 datas/,</br>
	 *                所有的创建索引的json文件名规则为index__[索引名称]__[类型]，</br>
	 *                所有的创建数据的json文件名规则为data__[索引名称]__[类型]，</br>
	 *                中间都是两个英文的下划线，内容必须是一个json数组，并且有不重复的id字段</br>
	 * 
	 *                创建索引的json文件内容示例（index-plan-plan.json）：</br>
	 * 
	 *                {</br>
	 *                "settings": {</br>
	 *                "number_of_shards": 5,</br>
	 *                "number_of_replicas": 0,</br>
	 *                "max_result_window": 200000000</br>
	 *                },</br>
	 *                "mappings": {</br>
	 *                "plan": {</br>
	 *                "properties": {</br>
	 *                "id": {</br>
	 *                "type": "string",</br>
	 *                "index": "not_analyzed"</br>
	 *                },</br>
	 *                "typeName": {</br>
	 *                "type": "string",</br>
	 *                "index": "not_analyzed"</br>
	 *                }</br>
	 *                }</br>
	 *                }</br>
	 *                }</br>
	 *                }</br>
	 * 
	 *                创建索引数据的文件内容示例（data-plan-plan.json）：</br>
	 * 
	 *                [{</br>
	 *                "id":"1",</br>
	 *                "typeName":"安保维稳"</br>
	 *                },{</br>
	 *                "id":"2",</br>
	 *                "typeName":"应急处置"</br>
	 *                } ]
	 * 
	 * 
	 * 
	 */
	public void init(String jsonDir) {
		this.jsonDir = (jsonDir == null ? "" : jsonDir);
		this.cls = getClass();
		createIndexIfNeed();
		createIndexDataIfNeed();
	}

	private void createIndexIfNeed() {

		try {
			File dir = new File(cls.getClassLoader().getResource(".").getFile()
					+ ("".equals(jsonDir.trim()) ? "indexs" : jsonDir + "/indexs"));

			if (!dir.exists()) {
				LOGGER.warn("未找到创建索引相关的文件,目录：{}", dir);
			} else {
				File[] indexJsonFiles = dir.listFiles();
				for (File file : indexJsonFiles) {
					createIndex(file);
				}
			}
		} catch (Exception e) {
			LOGGER.error("创建索引错误，在classPath中未发现路径:{}", jsonDir + "/indexs");
		}
	}

	private void createIndexDataIfNeed() {

		try {
			File dir = new File(cls.getClassLoader()
					.getResource("".equals(jsonDir.trim()) ? "datas" : jsonDir + "/datas").getFile());

			if (!dir.exists()) {
				LOGGER.warn("为找到创建索引相关的文件,目录：{}", dir);
			} else {
				File[] dataJsonFiles = dir.listFiles();
				for (File file : dataJsonFiles) {
					createIndexData(file);
				}
			}
		} catch (Exception e) {
			LOGGER.error("创建索引错误，在classPath中未发现路径:{}", jsonDir + "/datas");
		}
	}

	private void createIndex(File file) {
		String indexName = file.getName().substring(0, file.getName().lastIndexOf(".")).split("__")[1];
		LOGGER.info("正在检查索引,{}", indexName);

		String json = FileUtils.getContent(file);
		RequestBody body = RequestBody.create(mediaType, json);
		Request request = new Request.Builder()
				.url(String.format("http://%s:%s/%s", getIps()[0], getHttpPorts()[0], indexName)).post(body).build();
		try {
			Response response = client.newCall(request).execute();
			LOGGER.info("索引 {} 创建成功 ...", indexName);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}

	private void createIndexData(File file) {
		String indexName = file.getName().substring(0, file.getName().lastIndexOf(".")).split("__")[1];
		String tyepeName = file.getName().substring(0, file.getName().lastIndexOf(".")).split("__")[2];
		LOGGER.info("正在检查索引数据，索引：{}，类型：{}", indexName, tyepeName);

		String json = FileUtils.getContent(file);

		JSONArray array = JSON.parseArray(json);
		for (int i = 0; i < array.size(); i++) {
			JSONObject obj = array.getJSONObject(i);
			String id = obj.getString("id");

			RequestBody body = RequestBody.create(mediaType, obj.toJSONString());
			Request request = new Request.Builder().url(
					String.format("http://%s:%s/%s/%s/%s", getIps()[0], getHttpPorts()[0], indexName, tyepeName, id))
					.post(body).build();
			try {
				Response response = client.newCall(request).execute();
				LOGGER.info("索引 {}, 类型:{},创建数据成功:{} ...", indexName, tyepeName, obj);
			} catch (IOException e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	}

	public abstract String[] getIps();

	public abstract int[] getHttpPorts();

	public abstract int[] getTcpPorts();
}
