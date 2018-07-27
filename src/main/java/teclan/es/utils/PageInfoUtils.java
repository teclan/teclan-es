package teclan.es.utils;

import com.alibaba.fastjson.JSONObject;

public class PageInfoUtils {

	public static int getCurrentPage(JSONObject pageInfo) {
		return pageInfo.containsKey("currentPage") ? pageInfo.getIntValue("currentPage") : 1;
	}

	public static int getPageSize(JSONObject pageInfo) {
		return pageInfo.containsKey("pageSize") ? pageInfo.getIntValue("pageSize") : 25;
	}

	public static String getOrderBy(JSONObject pageInfo, String def) {
		return pageInfo.containsKey("orderBy") ? pageInfo.getString("orderBy") : def;
	}

	public static String getSort(JSONObject pageInfo) {
		return pageInfo.containsKey("sort") ? pageInfo.getString("sort") : "ASC";
	}

	public static int getTotalPages(long total, int pageSize) {
		return (int) Math.ceil(total * 1.0 / pageSize);
	}

	public static int getOffset(int currentPage, int pageSize, long totalPages) {

		if (totalPages > currentPage) {
			return (currentPage - 1) * pageSize;
		} else {
			return (int) (totalPages < 1 ? 0 : ((totalPages - 1) * pageSize));
		}

	}

	public static JSONObject getPageInfo(long total, int totalPages, int currentPage, int pageSize) {
		JSONObject pageInfo = new JSONObject();
		pageInfo.put("total", total);
		pageInfo.put("totalPages", totalPages);
		pageInfo.put("currentPage", currentPage > totalPages ? totalPages : currentPage);
		pageInfo.put("pageSize", pageSize);

		return pageInfo;
	}
}
