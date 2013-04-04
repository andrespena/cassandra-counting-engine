package com.sais.counting;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AlreadyExistsException;

/**
 * 
 * @author apenya
 * 
 */
public class CounterService {

	private static final String COUNTERS_COLUMN_FAMILY_NAME = "counters";
	
	private Session session;
	private ConsistencyLevel consistencyLevel;
	private WriteSynchronicity writeSynchronicity;

	/**
	 * Constructor.
	 * 
	 * @param hosts the hosts used as contact points
	 * @param keyspaceName the keyspace name
	 * @param columnFamilyName the column family (table) name
	 * @param consistencyLevel the default consistency level to be used
	 * @param writeSynchronicity the default write synchronicity to be used
	 */
	public CounterService(String hosts,
	                      String keyspaceName,
	                      ConsistencyLevel consistencyLevel,
	                      WriteSynchronicity writeSynchronicity) {
		if (StringUtils.isBlank(hosts)) {
			throw new IllegalArgumentException("Hosts must be specified");
		}
		if (StringUtils.isBlank(keyspaceName)) {
			throw new IllegalArgumentException("Keyspace name must be specified");
		}
		if (consistencyLevel == null) {
			throw new IllegalArgumentException("A not null consistency level is required");
		}
		if (writeSynchronicity == null) {
			throw new IllegalArgumentException("A not null write synchronicity is required");
		}
		Builder builder = Cluster.builder();
		builder.addContactPoints(hosts.split(","));
		Cluster cluster = builder.build();
		this.session = cluster.connect(keyspaceName);
		this.consistencyLevel = consistencyLevel;
		this.writeSynchronicity = writeSynchronicity;
		createSchema();
	}

	private void createSchema() {
		StringBuilder builder = new StringBuilder();
		try {
			builder.append("CREATE TABLE ")
			       .append(COUNTERS_COLUMN_FAMILY_NAME)
			       .append("( ")
			       .append("	name        varchar, ")
			       .append("	type        varchar, ")
			       .append("	granularity varchar, ")
			       .append("	time        timestamp, ")
			       .append("	value       counter, ")
			       .append("	PRIMARY KEY (name, type, granularity, time)" )
			       .append("); ");
			session.execute(builder.toString());
		} catch (AlreadyExistsException e) {
		}
	}

	/**
	 * Gets the {@link Counter} identified by the specified name, regardless of existence.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @return the {@link Counter} identified by the specified name, regardless of existence
	 */
	public Counter getCounter(String name) {
		return new Counter(session, COUNTERS_COLUMN_FAMILY_NAME, name, consistencyLevel, writeSynchronicity);
	}

}
