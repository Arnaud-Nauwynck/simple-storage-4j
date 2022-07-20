package org.simplestorage4j.api.util;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.val;

public class LoggingCounter {

	@NoArgsConstructor  @AllArgsConstructor
	public static class LoggingCounterParams {
	
		@Getter @Setter
		private int logFreq = 10_000;
	
		@Getter @Setter
		private int logMaxDelayMillis = 60 * 1000;
	
	}
	
	@FunctionalInterface
	public static interface MsgPrefixLoggingCallback {
//		public void logMessage(int count, long millis, //
//				long elapsedMillisSinceLastLog, int countSinceLastLog, long sumMillisSinceLastLog, //
//				String displayName);
		public void logWithMessagePrefix(String msgPrefix);
	}
	
	private final String displayName;
	
	@Getter @Setter
	private int logFreq = 10_000;

	@Getter @Setter
	private int logMaxDelayMillis = 60 * 1000;

	@Getter
	private int count;
	@Getter
	private long totalMillis;

	
	private int countLogModuloFreq = 0;
	
	private long lastLogTimeMillis;
	private int countSinceLastLog;
	private long sumMillisSinceLastLog;
	
	// ------------------------------------------------------------------------
	
	public LoggingCounter(String displayName) {
		this(displayName, new LoggingCounterParams());
	}

	public LoggingCounter(String displayName, LoggingCounterParams params) {
		this.displayName = displayName;
		this.logFreq = params.logFreq;
		this.logMaxDelayMillis = params.logMaxDelayMillis;
	}

	// ------------------------------------------------------------------------
	
	public synchronized void incr(long millis, MsgPrefixLoggingCallback msgPrefixLoggingCallback) {
		this.count++;
		this.totalMillis += millis;
		this.countLogModuloFreq--;
		this.countSinceLastLog++;		
		this.sumMillisSinceLastLog += millis;
		boolean log = false;
		if (countLogModuloFreq <= 0) {
			this.countLogModuloFreq = logFreq;
			log = true;
		}
		val now = System.currentTimeMillis();
		val elapsed =  now - lastLogTimeMillis;
		log = log || (elapsed > logMaxDelayMillis);
		if (log) {
			val msgPrefix = new StringBuilder();
			msgPrefix.append("(..");
			if (countSinceLastLog != 1) {
			    msgPrefix.append("+" + countSinceLastLog + "=");
			}
			msgPrefix.append("" + count);
			if (sumMillisSinceLastLog != 0) {
			    if (sumMillisSinceLastLog > 16) { // precision of jvm millis
			        msgPrefix.append(" : " + durationToString(sumMillisSinceLastLog));
			    }
			    if (sumMillisSinceLastLog != elapsed
			            && elapsed > 16 // ?
			            && lastLogTimeMillis != 0) {
			        msgPrefix.append(" since " + durationToString(elapsed));
			    }
			}
			msgPrefix.append(")");
			msgPrefix.append(" " + displayName);
			
			msgPrefixLoggingCallback.logWithMessagePrefix(msgPrefix.toString());
			
			this.countSinceLastLog = 0;
 			this.sumMillisSinceLastLog = 0;
 			this.lastLogTimeMillis = now;
		}
	}

	protected static String durationToString(long millis) {
		if (millis < 1000) {
			return millis + "ms";
		} else {
			val seconds = millis/1000;
			val remainMillis = millis - 1000 * seconds;
			String res = "";
			if (seconds <= 2 && remainMillis > 50) {
				res = remainMillis + "ms";
			}// else neglectable millis
			val minutes = seconds / 60;
			val remainSeconds = seconds - minutes * 60;
			if (minutes <= 2 && remainSeconds > 1) {
				res = remainSeconds + "s" + res;
			}// else neglectable seconds
			if (minutes != 0) {
				res = minutes + "mn" + res;
			}
			return res;
		}
	}
	
}
