package examples;

import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

public class SimplePartitioner implements Partitioner {

	@Override
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub

	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		int partition = 0;
		Integer numsPartition = cluster.partitionCountForTopic(topic);
		String k = (String) value;
		partition = Math.abs(k.hashCode()) % numsPartition;
		return partition;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}

}
