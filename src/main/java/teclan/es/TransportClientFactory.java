package teclan.es;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransportClientFactory {
	private static final Logger LOGGER = LoggerFactory.getLogger(TransportClientFactory.class);

	public static Map<String,TransportClient> CLIENTS = new HashMap<String,TransportClient> () ;

	public static TransportClient get(String clusterName, String[] ips, int[] ports) {
		
		if(CLIENTS.containsKey(clusterName)) {
			return CLIENTS.get(clusterName);
		}

		Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName)
				.put("client.transport.ignore_cluster_name", false).put("client.transport.ping_timeout", "5s")
				.put("client.transport.sniff", true).build();
		TransportClient client = null;
		try {
			client = TransportClient.builder().settings(settings).build();
			for (int i = 0; i < ips.length; i++) {
				client.addTransportAddress(
						new InetSocketTransportAddress(InetAddress.getByName(ips[i]), Integer.valueOf(ports[i])));
			}
		} catch (UnknownHostException e) {
			LOGGER.error(e.getMessage(), e);
		}
		CLIENTS.put(clusterName, client);

		return client;
	}

}
