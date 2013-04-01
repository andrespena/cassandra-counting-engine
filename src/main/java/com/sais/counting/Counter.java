package com.sais.counting;

import java.util.Date;

import org.joda.time.DateTime;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

public class Counter {

	
	private Session session;
	private String cfName;
	private String name;
	
	/**
	 * Constructor.
	 * 
	 * @param session the {@link Session} to be used
	 * @param cfName the column family name
	 * @param name the counter's name
	 */
	Counter(Session session, String cfName, String name) {
		if (session == null) {
			throw new IllegalArgumentException("A not null session is required");
		}
		if (cfName == null || cfName.isEmpty()) {
			throw new IllegalArgumentException("A not null or empty column family name is required");
		}
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("A not null or empty counter name is required");
		}
		this.session = session;
		this.cfName = cfName;
		this.name = name;
	}

	/**
	 * 
	 * @return
	 */
	public String getName() {
		return name;
	}
	
	/**
	 * Increase this in one unit for the current date.
	 * 
	 * @param transaction the atomic {@link Transaction} to be used
	 */
	public void update() {
		update(new Date(), null);		
	}
    
	/**
	 * Increase this in one unit for the specified date.
	 * 
	 * @param transaction the atomic {@link Transaction} to be used
	 * @param date
	 */
	public void update(Date date) {
		update(date, null);
	}
    
	/**
	 * Increase this in one unit for the current date using the specified event
	 * value for means, deviations and variances.
	 * 
	 * @param transaction the atomic {@link Transaction} to be used
	 * @param value the event's value for means, deviations and variances
	 */
	public void update(Long value) {
		update(new Date(), value);
	}

	/**
	 * Updates the value of this {@link Counter} and of all its ancestors using
	 * the specified value and date.
	 * 
	 * @param transaction the atomic {@link Transaction} to be used
	 * @param date the event's date
	 * @param value the event's value for means, deviations and variances
	 */
	public void update(Date date, Long value) {			
		StringBuilder builder = new StringBuilder();
		builder.append("BEGIN COUNTER BATCH\n");
		for (TimeGranularity granularity : TimeGranularity.values()) {
			builder.append(update(ValueType.COUNTS, granularity, date, 1L));
			if (value != null) {
				builder.append(update(ValueType.SUMS, granularity, date, value));
				builder.append(update(ValueType.SQUARES, granularity, date, value * value));
			}
		}
		builder.append("APPLY BATCH;\n");
		session.execute(builder.toString());
	}
	
	private String update(ValueType type, TimeGranularity granularity, Date date, Long value) {
		Update update = QueryBuilder.update(cfName);
		update.setConsistencyLevel(ConsistencyLevel.QUORUM);
		update.where(QueryBuilder.eq("name", name));
		update.where(QueryBuilder.eq("type", type.getCode()));
		update.where(QueryBuilder.eq("granularity", mapTimeGranularityName(granularity)));
		update.where(QueryBuilder.eq("time", normalizeDate(granularity, date)));
		update.with(QueryBuilder.incr("value", value));
		return '\t' + update.getQueryString().replace(';', '\n');
	}

	/**
	 * Deletes this from database.
	 * 
	 * @param transaction the atomic {@link Transaction} to be used
	 */
	public void delete() {
		Delete delete = QueryBuilder.delete().from(cfName);
		delete.setConsistencyLevel(ConsistencyLevel.QUORUM);
		delete.where(QueryBuilder.eq("name", name));
		session.execute(delete);
	}

//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see
//	 * com.g4m3.platform.analytics.Counter#getStats(com.g4m3.platform.analytics
//	 * .Counter.StatType, com.g4m3.platform.analytics.Counter.TimeGranularity,
//	 * java.util.Date, java.util.Date)
//	 */
//	@Override
//	public Map<Date, Double> getStats(StatType stat, TimeGranularity granularity, Date start, Date finish) {
//		switch (stat) {
//		case COUNTS:
//			return getCounts(granularity, start, finish);
//		case SUMS:
//			return getSums(granularity, start, finish);
//		case SQUARES:
//			return getSquares(granularity, start, finish);
//		case MEANS:
//			return getMeans(granularity, start, finish);
//		case DEVIATIONS:
//			return getDeviations(granularity, start, finish);
//		case VARIANCES:
//			return getVariances(granularity, start, finish);
//		default:
//			throw new RuntimeException();
//		}
//	}
//
//	private Map<Date, Double> getCounts(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.COUNTS, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getSums(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.SUMS, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getSquares(TimeGranularity granularity, Date start, Date finish) {
//		return queryValues(ValueType.SQUARES, granularity, start, finish);
//	}
//
//	private Map<Date, Double> getMeans(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> counts = getCounts(granularity, start, finish);
//		Map<Date, Double> sums = getSums(granularity, start, finish);
//		Map<Date, Double> means = new HashMap<Date, Double>(counts.size());
//		for (Date time : counts.keySet()) {
//			double count = counts.get(time);
//			double sum = sums.get(time);
//			double mean = sum / count;
//			means.put(time, mean);
//		}
//		return means;
//	}
//
//	private Map<Date, Double> getDeviations(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> counts = getCounts(granularity, start, finish);
//		Map<Date, Double> sums = getSums(granularity, start, finish);
//		Map<Date, Double> squares = getSquares(granularity, start, finish);
//		Map<Date, Double> deviations = new HashMap<Date, Double>(counts.size());
//		for (Date time : counts.keySet()) {
//			double count = counts.get(time);
//			double sum = sums.get(time);
//			double square = squares.get(time);
//			double deviation = 0.0;
//			if (count > 1) {
//				deviation = Math.sqrt((square - sum * sum / count) / (count - 1));
//			}
//			deviations.put(time, deviation);
//		}
//		return deviations;
//	}
//
//	private Map<Date, Double> getVariances(TimeGranularity granularity, Date start, Date finish) {
//		Map<Date, Double> deviations = getDeviations(granularity, start, finish);
//		Map<Date, Double> variances = new HashMap<Date, Double>(deviations.size());
//		for (Date time : deviations.keySet()) {
//			double deviation = deviations.get(time);
//			double variance = deviation * deviation;
//			variances.put(time, variance);
//		}
//		return variances;
//
//	}
//
//	private Map<Date, Double> queryValues(ValueType valueType, TimeGranularity granularity, Date start, Date finish) {
//
//		// Build row key
//		String rowKey = getRowKey();
//
//		// Build range column names
//		Composite startColName = getColumnName(valueType, granularity, start, true);
//		Composite finishColName = getColumnName(valueType, granularity, finish, false);
//
//		// Setup query
//		SliceCounterQuery<String, Composite> query = HFactory.createCounterSliceQuery(keyspace, ss, cs);
//		query.setKey(rowKey);
//		query.setColumnFamily(cfName);
//		query.setRange(startColName, finishColName, false, 1000);
//
//		// Run query
//		QueryResult<CounterSlice<Composite>> queryResult = query.execute();
//
//		// Parse query
//		CounterSlice<Composite> counterSlice = queryResult.get();
//		List<HCounterColumn<Composite>> columns = counterSlice.getColumns();
//		Map<Date, Double> result = new HashMap<Date, Double>(columns.size());
//		for (HCounterColumn<Composite> column : columns) {
//			Composite columnName = column.getName();
//			Date counterDate = columnName.get(2, das);
//			Long counterValue = column.getValue();
//			result.put(counterDate, counterValue.doubleValue());
//		}
//
//		// Return result
//		return result;
//	}
//
//	/*
//	 * (non-Javadoc)
//	 * 
//	 * @see com.g4m3.platform.analytics.Counter#toString()
//	 */
//	@Override
//	public String toString() {
//		StringBuilder builder = new StringBuilder();
//		builder.append(name);
//		return builder.toString();
//	}

	/**
	 * Enumerated type representing the type of a value.
	 */
	private static enum ValueType {

		COUNTS("counts"), SUMS("sums"), SQUARES("squares");

		private String code;

		private ValueType(String code) {
			this.code = code;
		}

		private String getCode() {
			return code;
		}
	}

	private String mapTimeGranularityName(TimeGranularity granularity) {
		switch (granularity) {
		case ALL:
			return "all";
		case MINUTELY:
			return "minutelly";
		case HOURLY:
			return "hourly";
		case DAILY:
			return "daily";
		case MONTHLY:
			return "monthly";
		case YEARLY:
			return "yearly";
		default:
			throw new RuntimeException();
		}
	}

	private Date normalizeDate(TimeGranularity granularity, Date date) {
		DateTime dateTime = new DateTime(date);
		switch (granularity) {
		case MINUTELY:
			return new DateTime(dateTime.getYear(),
			                    dateTime.getMonthOfYear(),
			                    dateTime.getDayOfMonth(),
			                    dateTime.getHourOfDay(),
			                    dateTime.getMinuteOfHour()).toDate();
		case HOURLY:
			return new DateTime(dateTime.getYear(),
			                    dateTime.getMonthOfYear(),
			                    dateTime.getDayOfMonth(),
			                    dateTime.getHourOfDay(),
			                    0).toDate();
		case DAILY:
			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), dateTime.getDayOfMonth(), 0, 0).toDate();
		case MONTHLY:
			return new DateTime(dateTime.getYear(), dateTime.getMonthOfYear(), 1, 0, 0).toDate();
		case YEARLY:
			return new DateTime(dateTime.getYear(), 1, 1, 0, 0).toDate();
		case ALL:
			return new DateTime(0).toDate();
		default:
			throw new RuntimeException();
		}
	}

	/**
	 * Enumerated type representing the type of a statistical indicator.
	 */
	static enum StatType {
		COUNTS, SUMS, SQUARES, MEANS, DEVIATIONS, VARIANCES;
	}

	/**
	 * 
	 * Enumerated type representing a time's granularity.
	 * 
	 */
	static enum TimeGranularity {
		ALL, MINUTELY, HOURLY, DAILY, MONTHLY, YEARLY;
	}

}
