package com.sais.counting;

public enum ConsistencyLevel {

	ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_QUORUM, EACH_QUORUM;
	
	static ConsistencyLevel parseDatastax(com.datastax.driver.core.ConsistencyLevel cl) {
		if (cl == null) return ONE;
		switch (cl) {
			case ANY: return ANY;
	        case ONE: return ONE;
	        case TWO: return TWO;
	        case THREE: return THREE;
	        case QUORUM: return QUORUM;
	        case ALL: return ALL;
	        case LOCAL_QUORUM: return LOCAL_QUORUM;
	        case EACH_QUORUM: return EACH_QUORUM;
		}
		throw new AssertionError();
	}
	
	com.datastax.driver.core.ConsistencyLevel toDatastax() {
		switch (this) {
			case ANY: return com.datastax.driver.core.ConsistencyLevel.ANY;
	        case ONE: return com.datastax.driver.core.ConsistencyLevel.ONE;
	        case TWO: return com.datastax.driver.core.ConsistencyLevel.TWO;
	        case THREE: return com.datastax.driver.core.ConsistencyLevel.THREE;
	        case QUORUM: return com.datastax.driver.core.ConsistencyLevel.QUORUM;
	        case ALL: return com.datastax.driver.core.ConsistencyLevel.ALL;
	        case LOCAL_QUORUM: return com.datastax.driver.core.ConsistencyLevel.LOCAL_QUORUM;
	        case EACH_QUORUM: return com.datastax.driver.core.ConsistencyLevel.EACH_QUORUM;
		}
		throw new AssertionError();
	}
}
