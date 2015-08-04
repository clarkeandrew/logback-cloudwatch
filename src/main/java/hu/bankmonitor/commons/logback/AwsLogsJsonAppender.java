package hu.bankmonitor.commons.logback;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.CreateLogGroupRequest;
import com.amazonaws.services.logs.model.CreateLogStreamRequest;
import com.amazonaws.services.logs.model.InputLogEvent;
import com.amazonaws.services.logs.model.InvalidSequenceTokenException;
import com.amazonaws.services.logs.model.PutLogEventsRequest;
import com.amazonaws.services.logs.model.PutLogEventsResult;
import com.amazonaws.services.logs.model.ResourceAlreadyExistsException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;

import lombok.Setter;

@Setter
public class AwsLogsJsonAppender extends AppenderBase<ILoggingEvent> {

	static enum BatchMode {
		none,
		seconds,
		count
	};

	private final ObjectMapper om;

	private AWSLogsClient awsLogsClient;

	private String lastSequenceToken;

	private String awsAccessKey;

	private String awsSecretKey;

	private String awsRegionName;

	private String logGroupName = "test-log-group";

	private String logStreamName = "test-log-stream";

	private BatchMode batchMode = BatchMode.none;

	private int batchModeValue = 0;

	private long lastUpdateMillis = 0;

	List<InputLogEvent> logEvents;
	public AwsLogsJsonAppender() {

		om = new ObjectMapper();
		om.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
		JacksonUtils.registerModulesToObjectMapper(om);
	}

	@Override
	protected void append(ILoggingEvent eventObject) {

		try {
			sendEvent(om.writeValueAsString(LogbackUtils.iLoggingEvent2Map(eventObject)));
		} catch (Exception e) {
			e.printStackTrace();
			addError("Error while sending a message", e);
		}
	}

	@Override
	public void stop() {
		if (logEvents.size() > 0) {
			sendEventBatch();
		}
		super.stop();
	}

	@Override
	public void start() {

		awsLogsClient = new AWSLogsClient(new StaticCredentialsProvider(new BasicAWSCredentials(awsAccessKey, awsSecretKey)));
		if (awsRegionName != null) {
			awsLogsClient.setRegion(Region.getRegion(Regions.fromName(awsRegionName)));
		}

		CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest(logGroupName);
		try {
			awsLogsClient.createLogGroup(createLogGroupRequest);
		} catch (ResourceAlreadyExistsException e) {
			addInfo("Log group " + logGroupName + " already exists");
		}

		CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest(logGroupName, logStreamName);
		try {
			awsLogsClient.createLogStream(createLogStreamRequest);
		} catch (ResourceAlreadyExistsException e) {
			addInfo("Log stream " + logStreamName + "already exists");
		}

		try {
			sendEvent("Getting the next expected sequenceToken for AwsLogsAppender");
		} catch (InvalidSequenceTokenException e) {
			lastSequenceToken = e.getExpectedSequenceToken();
		}

		super.start();
	}

	private void sendEvent(String message) {
		addEventToBatch(message);
		switch (batchMode) {
			case count:
				if (logEvents.size() >= batchModeValue) {
					sendEventBatch();
				}
				break;
			case seconds:
				if (System.currentTimeMillis() - lastUpdateMillis > batchModeValue * 1000) {
					sendEventBatch();
					lastUpdateMillis = System.currentTimeMillis();
				}
				break;
			default:
				sendEventBatch();
				break;
		}
	}

	private void addEventToBatch(String message) {
		logEvents.add(new InputLogEvent().withTimestamp(new Date().getTime()).withMessage(message));
	}

	private void sendEventBatch() {
		PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(logGroupName, logStreamName, logEvents);
		putLogEventsRequest.setSequenceToken(lastSequenceToken);
		PutLogEventsResult putLogEventsResult = awsLogsClient.putLogEvents(putLogEventsRequest);
		lastSequenceToken = putLogEventsResult.getNextSequenceToken();
	}
}
