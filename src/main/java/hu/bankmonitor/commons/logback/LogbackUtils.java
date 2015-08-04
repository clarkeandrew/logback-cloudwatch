package hu.bankmonitor.commons.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lombok.experimental.UtilityClass;

@UtilityClass
public class LogbackUtils {

	public static Map<String, Object> iLoggingEvent2Map(ILoggingEvent eventObject) {

		StackTraceElement[] callerData = null;
		if (eventObject.hasCallerData()) {
			callerData = eventObject.getCallerData();

		}

		Map<String, Object> map = new HashMap<>();
		map.put("threadName", eventObject.getThreadName());
		map.put("level", eventObject.getLevel());
		map.put("message", eventObject.getMessage());
		map.put("argumentArray", eventObject.getArgumentArray());
		map.put("formattedMessage", eventObject.getFormattedMessage());
		map.put("loggerName", eventObject.getLoggerName());
		map.put("loggerContextVO", eventObject.getLoggerContextVO());
		map.put("throwableProxy", getThrowableProxyMap(eventObject.getThrowableProxy()));
		map.put("callerData", callerData);
		map.put("markers", getMarkers(eventObject));
		map.put("mdcPropertyMap", eventObject.getMDCPropertyMap());
		map.put("timeStamp", eventObject.getTimeStamp());

		return map;
	}

	private static Map<String, Object> getThrowableProxyMap(IThrowableProxy iThrowableProxy) {

		Map<String, Object> throwableProxyMap = null;
		if (iThrowableProxy != null) {
			throwableProxyMap = new HashMap<>();
			throwableProxyMap.put("message", iThrowableProxy.getMessage());
			throwableProxyMap.put("className", iThrowableProxy.getClassName());
			throwableProxyMap.put("stackTraceElements", iThrowableProxy.getStackTraceElementProxyArray());
			throwableProxyMap.put("commonFrames", iThrowableProxy.getCommonFrames());
			throwableProxyMap.put("cause", getThrowableProxyMap(iThrowableProxy.getCause()));
			throwableProxyMap.put("suppressed", iThrowableProxy.getSuppressed());
		}

		return throwableProxyMap;
	}

	private static Set<String> getMarkers(ILoggingEvent eventObject) {

		if (eventObject.getMarker() == null) {
			return Collections.emptySet();
		}

		Set<String> markers = new HashSet<>();
		Iterator<?> i = eventObject.getMarker().iterator();
		while (i.hasNext()) {
			markers.add(i.next().toString());
		}

		return markers;
	}

}
