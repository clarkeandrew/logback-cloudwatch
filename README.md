# Logback appenders for AWS

## AwsLogsAppender

Send logs to Amazon CloudWatch Logs.

### Requirements:
 - Amazon IAM user with 'CloudWatchLogsFullAccess' (arn:aws:iam::aws:policy/CloudWatchLogsFullAccess) policy

### Download: [ ![Download](https://api.bintray.com/packages/bankmonitor/hu.bankmonitor.commons/logback-appenders/images/download.svg) ](https://bintray.com/bankmonitor/hu.bankmonitor.commons/logback-appenders/_latestVersion)

### Maven:

``` xml
<repositories>
	<repository>
		<id>bintray-bankmonitor-hu.bankmonitor.commons</id>
		<name>bintray</name>
		<url>http://dl.bintray.com/bankmonitor/hu.bankmonitor.commons</url>
		<snapshots>
			<enabled>false</enabled>
		</snapshots>
	</repository>
</repositories>

<dependencies>
	<dependency>
		<groupId>hu.bankmonitor.commons</groupId>
		<artifactId>logback-appenders</artifactId>
		<version>0.0.4</version>
		<exclusions>
			<exclusion>
				<artifactId>commons-logging</artifactId>
				<groupId>commons-logging</groupId>
			</exclusion>
		</exclusions>
	</dependency>
</dependencies>
```

If you use [jcl-over-slf4j](http://www.slf4j.org/legacy.html) then exclude `commons-logging`:

``` xml
<dependencies>
	<dependency>
		<groupId>hu.bankmonitor.commons</groupId>
		<artifactId>logback-appenders</artifactId>
		<version>0.0.4</version>
		<exclusions>
			<exclusion>
				<artifactId>commons-logging</artifactId>
				<groupId>commons-logging</groupId>
			</exclusion>
		</exclusions>
	</dependency>
</dependencies>
```

### Usage:

``` xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

	<appender name="AWS_LOGS" class="hu.bankmonitor.commons.logback.AwsLogsJsonAppender">
		<awsAccessKey>yourAwsAccessKey</awsAccessKey>
		<awsSecretKey>yourAwsSecretKey</awsSecretKey>
		<awsRegionName>region</awsRegionName>
		<logGroupName>log-group-name</logGroupName>
		<logStreamName>log-stream-name</logStreamName>
	</appender>

	<root level="DEBUG">
		<appender-ref ref="CONSOLE" />
		<appender-ref ref="AWS_LOGS" />
	</root>
	
</configuration>
```


#### Properties:

| Property      | Required  | Description                                              |
| :------------ | :-------: | :------------------------------------------------------- |
| awsAccessKey  | yes       | AWS access key                                           |
| awsSecretKey  | yes       | AWS secret key                                           |
| awsRegionName | no        | CloudWatch region name. Default: US_WEST_2               |
| logGroupName  | no        | CloudWatch log group name. Default: log-group-name       |
| logStreamName | no        | CloudWatch stream name. Default: log-stream-name         |
