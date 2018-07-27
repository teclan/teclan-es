package teclan.es.utils;

import com.relops.snowflake.Snowflake;

/**
 * Twitter的雪花算法SnowFlake，用来生成64位的ID
 * 
 * @author dev
 *
 */
public class IdGenerater {

	private static int NODE = 1;
	private static Snowflake SNOWFLAKE;

	public static void init(int node) {
		NODE=node;
		SNOWFLAKE = new Snowflake(NODE);
	}

	public static synchronized String getNextId() {
		return String.valueOf(SNOWFLAKE.next());
	}

	public Snowflake getSnowflake() {
		if (SNOWFLAKE == null) {
			SNOWFLAKE = new Snowflake(NODE);
		}
		return SNOWFLAKE;
	}

}
