package com.sais.counting;

import java.util.Date;

import org.apache.commons.lang.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;

public class CounterService {
	
	private String columnFamilyName;
	private Session session;

	/**
	 * Constructor.
	 * 
	 * @param hosts
	 * @param keyspaceName
	 * @param columnFamilyName
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
	 * Gets the counter identified by the specified name.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @return the {@link Counter} identified by the specified name
	 */
	public Counter getCounter(String name) {
		return new Counter(session, columnFamilyName, name);
	}
	
	/**
	 * Increase the {@link Counter} identified by the specified name in one unit for the current date.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 */
	public void update(String name) {
		Counter counter = getCounter(name);
		counter.update();
	}
    
	/**
	 * Increase the {@link Counter} identified by the specified name in one unit for the specified date.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @param date the event's date
	 */
	public void update(String name, Date date) {
		Counter counter = getCounter(name);
		counter.update(date);
	}
    
	/**
	 * Increase the {@link Counter} identified by the specified name in one unit for the current date using the specified event
	 * value for means, deviations and variances.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @param value the event's value for means, deviations and variances
	 */
	public void update(String name, Long value) {
		Counter counter = getCounter(name);
		counter.update(value);
	}

	/**
	 * Updates the value of the {@link Counter} identified by the specified name using
	 * the specified value and date.
	 * 
	 * @param name the {@link Counter}'s identifying name
	 * @param date the event's date
	 * @param value the event's value for means, deviations and variances
	 */
	public void update(String name, Date date, Long value) {
		Counter counter = getCounter(name);
		counter.update(date, value);
	}

}
