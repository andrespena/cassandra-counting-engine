package com.sais.counting;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.joda.time.DateTime;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Class representing a distributed historic event counter, including several historic statistic
 * indicators over event's values as means, deviations and variances.
 * 
 * @author apenya
 * 
 */
public class Counter {

	private Session session;
	private String cfName;
	private String name;

	private static final com.datastax.driver.core.ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = com.datastax.driver.core.ConsistencyLevel.QUORUM;
	private com.datastax.driver.core.ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;
	
	private static final boolean DEFAULT_SYNCHRONOUS_WRITES = true;
	private boolean synchronousWrites = DEFAULT_SYNCHRONOUS_WRITES;

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
	 * Gets the identifying name.
	 * 
	 * @return the identifying name
	 */
	public String getName() {
		return name;
	}

	/**
	 * Sets the consistency level to be used both writings and readings
	 * 
	 * @return the consistency level to be used both writings and readings
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return ConsistencyLevel.parseCQLDriverCL(consistencyLevel);
	}

	/**
	 * Gets the consistency level to be used both writings and readings.
	 * 
	 * @param consistencyLevel the consistency level to be used both writings and readings to set
	 */
	public Counter setConsistencyLevel(ConsistencyLevel consistencyLevel) {
		if (consistencyLevel == null)
			throw new NullPointerException("Consistency level must be not null");
		this.consistencyLevel = consistencyLevel.toCQLDriverCL();
		return this;
	}

	/**
	 * Return {@code true} if this is setup for synchronous writes, {@code false} otherwise.
	 * @return {@code true} if this is setup for synchronous writes, {@code false} otherwise
	 */
	public boolean getSynchronousWrites() {
		return synchronousWrites;
	}

	/**
	 * Sets if writes must be synchronous
	 * @param synchronousWrites if writes must be synchronous
	 */
	public Counter setSynchronousWrites(boolean synchronousWrites) {
		this.synchronousWrites = synchronousWrites;
		return this;
	}

	/**
	 * Increase this {@link Counter} in one unit for the current date.
	 */
	public void update() {
		update(new Date(), null);
	}

	/**
	 * Increase this {@link Counter} in one unit for the specified date.
	 * 
	 * @param date the event's date
	 */
	public void update(Date date) {
		update(date, null);
	}

	/**
	 * Updates the value of this {@link Counter} using the specified value and the current date.
	 * 
	 * @param value the event's value for means, deviations and variances
	 */
	public void update(Long value) {
		update(new Date(), value);
	}

	/**
	 * Updates the value of this {@link Counter} using the specified value and date.
	 * 
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
		String query = builder.toString();
		if (synchronousWrites) {
			session.execute(query);
		} else {
			session.executeAsync(query);
		}
	}

	/**
	 * Updates the value of this {@link Counter} using the specified value type, value and date.
	 * 
	 * @param valueType the type of the value to be updated
	 * @param date the event's date
	 * @param value the event's value for means, deviations and variances
	 */
	private String update(ValueType valueType, TimeGranularity granularity, Date date, Long value) {
		Update update = QueryBuilder.update(cfName);
		update.setConsistencyLevel(consistencyLevel);
		update.where(QueryBuilder.eq("name", name));
		update.where(QueryBuilder.eq("type", valueType.getCode()));
		update.where(QueryBuilder.eq("granularity", granularity.getCode()));
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
		delete.setConsistencyLevel(consistencyLevel);
		delete.where(QueryBuilder.eq("name", name));
		if (synchronousWrites) {
			session.execute(delete);
		} else {
			session.executeAsync(delete);
		}
	}

	/**
	 * Gets the map of value counts by date for the specified time granularity and date range.
	 * 
	 * @param granularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value counts by date for the specified time granularity and date range
	 */
	public Map<Date, Double> getCounts(TimeGranularity granularity, Date startDate, Date finishDate) {
		return queryValues(ValueType.COUNTS, granularity, startDate, finishDate);
	}

	/**
	 * Gets the map of value sums by date for the specified time granularity and date range.
	 * 
	 * @param granularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value sums by date for the specified time granularity and date range
	 */
	public Map<Date, Double> getSums(TimeGranularity granularity, Date startDate, Date finishDate) {
		return queryValues(ValueType.SUMS, granularity, startDate, finishDate);
	}

	/**
	 * Gets the map of value squares by date for the specified time granularity and date range.
	 * 
	 * @param granularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value squares by date for the specified time granularity and date range
	 */
	public Map<Date, Double> getSquares(TimeGranularity granularity, Date startDate, Date finishDate) {
		return queryValues(ValueType.SQUARES, granularity, startDate, finishDate);
	}

	/**
	 * Gets the map of value means by date for the specified time granularity and date range.
	 * 
	 * @param granularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value means by date for the specified time granularity and date range
	 */
	public Map<Date, Double> getMeans(TimeGranularity granularity, Date startDate, Date finishDate) {
		Map<Date, Double> counts = getCounts(granularity, startDate, finishDate);
		Map<Date, Double> sums = getSums(granularity, startDate, finishDate);
		Map<Date, Double> means = new HashMap<Date, Double>(counts.size());
		for (Date time : counts.keySet()) {
			double count = counts.get(time);
			double sum = sums.get(time);
			double mean = sum / count;
			means.put(time, mean);
		}
		return means;
	}

	/**
	 * Gets the map of value standard deviations by date for the specified time granularity and date
	 * range.
	 * 
	 * @param timeGranularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value standard deviations by date for the specified time granularity and
	 *         date range
	 */
	public Map<Date, Double> getDeviations(TimeGranularity timeGranularity, Date startDate, Date finishDate) {
		Map<Date, Double> counts = getCounts(timeGranularity, startDate, finishDate);
		Map<Date, Double> sums = getSums(timeGranularity, startDate, finishDate);
		Map<Date, Double> squares = getSquares(timeGranularity, startDate, finishDate);
		Map<Date, Double> deviations = new HashMap<Date, Double>(counts.size());
		for (Date date : counts.keySet()) {
			double count = counts.get(date);
			double sum = sums.get(date);
			double square = squares.get(date);
			double deviation = 0.0;
			if (count > 1) {
				deviation = Math.sqrt((square - sum * sum / count) / (count - 1));
			}
			deviations.put(date, deviation);
		}
		return deviations;
	}

	/**
	 * Gets the map of value variances by date for the specified time granularity and date range.
	 * 
	 * @param timeGranularity the time granularity
	 * @param startDate the date range start, included
	 * @param finishDate the date range finish, included
	 * @return the map of value variances by date for the specified time granularity and date range
	 */
	public Map<Date, Double> getVariances(TimeGranularity timeGranularity, Date startDate, Date finishDate) {
		Map<Date, Double> deviations = getDeviations(timeGranularity, startDate, finishDate);
		Map<Date, Double> variances = new HashMap<Date, Double>(deviations.size());
		for (Date time : deviations.keySet()) {
			double deviation = deviations.get(time);
			double variance = deviation * deviation;
			variances.put(time, variance);
		}
		return variances;

	}

	private Map<Date, Double> queryValues(ValueType valueType,
	                                      TimeGranularity timeGranularity,
	                                      Date startDate,
	                                      Date finishDate) {
		Select select = QueryBuilder.select("time", "value")
		                            .from(cfName)
		                            .where(eq("name", name))
		                            .and(eq("type", valueType.getCode()))
		                            .and(eq("granularity", timeGranularity.getCode()))
		                            .and(gte("time", normalizeDate(timeGranularity, startDate)))
		                            .and(lte("time", normalizeDate(timeGranularity, finishDate)))
		                            .orderBy(asc("type"), asc("granularity"), asc("time"));
		select.setConsistencyLevel(consistencyLevel);
		System.out.println("QUERY: " + select.getQueryString());
		Map<Date, Double> result = new LinkedHashMap<Date, Double>();
		for (Row row : session.execute(select)) {
			Date date = row.getDate("time");
			Long value = row.getLong("value");
			result.put(date, value.doubleValue());
		}
		return result;
	}

	/**
	 * Gets the normalized {@link Date} for the specified {@link TimeGranularity} and {@link Date}.
	 * 
	 * @param timeGranularity the {@link TimeGranularity}
	 * @param date the {@link Date} to be normalized
	 * @return the normalized {@link Date}
	 */
	private Date normalizeDate(TimeGranularity timeGranularity, Date date) {
		DateTime dt = new DateTime(date);
		switch (timeGranularity) {
		case MINUTELY:
			return new DateTime(dt.getYear(),
			                    dt.getMonthOfYear(),
			                    dt.getDayOfMonth(),
			                    dt.getHourOfDay(),
			                    dt.getMinuteOfHour()).toDate();
		case HOURLY:
			return new DateTime(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), dt.getHourOfDay(), 0).toDate();
		case DAILY:
			return new DateTime(dt.getYear(), dt.getMonthOfYear(), dt.getDayOfMonth(), 0, 0).toDate();
		case MONTHLY:
			return new DateTime(dt.getYear(), dt.getMonthOfYear(), 1, 0, 0).toDate();
		case YEARLY:
			return new DateTime(dt.getYear(), 1, 1, 0, 0).toDate();
		case ALL:
			return new DateTime(0).toDate();
		default:
			throw new RuntimeException();
		}
	}

	/**
	 * Enumerated type representing the type of a value.
	 */
	public static enum ValueType {

		COUNTS("counts"), SUMS("sums"), SQUARES("squares");

		private String code;

		private ValueType(String code) {
			this.code = code;
		}

		private String getCode() {
			return code;
		}
	}

	/**
	 * 
	 * Enumerated type representing a time's granularity.
	 * 
	 */
	public static enum TimeGranularity {

		ALL("all"), MINUTELY("minutelly"), HOURLY("hourly"), DAILY("daily"), MONTHLY("monthly"), YEARLY("yearly");

		private String code;

		private TimeGranularity(String code) {
			this.code = code;
		}

		private String getCode() {
			return code;
		}
	}

}
