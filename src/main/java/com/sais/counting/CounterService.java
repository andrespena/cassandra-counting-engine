package com.sais.counting;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

/**
 * 
 * @author apenya
 *
 */
public class CounterService {
	
	private String columnFamilyName;
	private Session session;

	/**
	 * Constructor.
	 * 
	 * @param hosts the hosts used as contact points
	 * @param keyspaceName the keyspace name
	 * @param columnFamilyName the column family (table) name
	 */
	public CounterService(String hosts, String keyspaceName, String columnFamilyName) {
		if (StringUtils.isBlank(hosts)) {
			throw new IllegalArgumentException("Hosts must be specified");
		}
		if (StringUtils.isBlank(keyspaceName)) {
			throw new IllegalArgumentException("Keyspace name must be specified");
		}
		if (StringUtils.isBlank(columnFamilyName)) {
			throw new IllegalArgumentException("Column family name must be specified");
		}
		this.columnFamilyName = columnFamilyName;
		Builder builder = Cluster.builder();
		builder.addContactPoints(hosts.split(","));
		Cluster cluster = builder.build();
		this.session = cluster.connect(keyspaceName);
    }
	
	/**
	 * Gets the {@link Counter} identified by the specified name, regardless of existence.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @return the {@link Counter} identified by the specified name, regardless of existence
	 */
	public Counter getCounter(String name) {
		return new Counter(session, columnFamilyName, name);
	}

}
