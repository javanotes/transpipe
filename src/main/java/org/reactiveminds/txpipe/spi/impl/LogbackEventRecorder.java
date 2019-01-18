package org.reactiveminds.txpipe.spi.impl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Observable;
import java.util.Observer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.spi.EventRecord;
import org.reactiveminds.txpipe.spi.EventRecorder;
import org.reactiveminds.txpipe.utils.Helper;
import org.reactiveminds.txpipe.utils.TimeChangeNotifier;
import org.reactiveminds.txpipe.utils.TimeChangeNotifier.HourlyNotifier;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.FileAppender;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;

/**
 * A default implementation of {@linkplain EventRecorder} that logs events to file using logback.
 * 
 * @author Sutanu_Dalui
 *
 */
public class LogbackEventRecorder implements EventRecorder {

	private static final String FOLDER_PATTERN = "yyyy-MM-dd";
	private static final String FILE_PATTERN = "{HH}.log";
	private final static String ROLLING_FOLDER_PATTERN = "/%d{"+FOLDER_PATTERN+"}/";
	private final static String ROLLING_FILE_PATTERN = "-%d"+FILE_PATTERN;
	private static final String LOGGER_NAME = "EventLog";
	
	private static String folderName() {
		return new SimpleDateFormat(FOLDER_PATTERN).format(new Date());
	}
	private static String fileSuffix() {
		String dtFmt = Helper.extractWithinTags(FILE_PATTERN, '{', '}');
		return Helper.replaceWithinTags(FILE_PATTERN, new SimpleDateFormat(dtFmt).format(new Date()), '{', '}');
	}

	@Value("${txpipe.event.recorder.logDir:./logs/events/}")
	private String loggingDir;
	@Value("${txpipe.event.recorder.logFileName:" + LOGGER_NAME + "}")
	private String loggingFile;
	@Value("${txpipe.event.recorder.logPattern:%date %msg%n}")
	private String logPattern;
	
	@Value("${txpipe.event.enableTimer:true}")
	private boolean enableTimer;

	private TimeChangeNotifier timer;
	private volatile Logger eventLogger = null;
	
	@PostConstruct
	private void onInit() {
		configureFileLogger();
		if (enableTimer) {
			timer = new HourlyNotifier();
			timer.addObserver(new Observer() {

				@Override
				public void update(Observable o, Object arg) {
					configureFileLogger();
				}
			});
			timer.start();
		}

	}

	@PreDestroy
	private void onStop() {
		if (timer != null) {
			timer.stop();
		}
	}

	private void configureFileLogger() {
		if (enableTimer) {
			eventLogger = createLogger(LOGGER_NAME, loggingDir + folderName() + File.separator + loggingFile + "_"+fileSuffix(), logPattern);
		} else {
			eventLogger = createRollingLogger(LOGGER_NAME, loggingFile, loggingDir, logPattern);
		}
	}

	/**
	 * Use the rolling file appender. TODO: use rolling file?
	 * 
	 * @param loggerName
	 * @param filePath
	 * @param pattern 
	 * @return
	 */
	private static Logger createRollingLogger(String loggerName, String file, String dirPath, String pattern) {
		LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();

		PatternLayoutEncoder ple = new PatternLayoutEncoder();
		ple.setPattern(pattern);
		ple.setContext(context);
		ple.start();

		RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
		fileAppender.setFile(dirPath + File.separator + file + ".log");
		fileAppender.setEncoder(ple);
		fileAppender.setContext(context);

		TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<>();
		policy.setContext(context);
		policy.setMaxHistory(24);
		policy.setFileNamePattern(dirPath + ROLLING_FOLDER_PATTERN + file + ROLLING_FILE_PATTERN);

		fileAppender.setRollingPolicy(policy);

		policy.setParent(fileAppender);
		policy.start();

		fileAppender.start();

		Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
		logger.setAdditive(false);
		logger.addAppender(fileAppender);
		logger.setLevel(Level.ALL);

		return logger;
	}

	/**
	 * Use a custom logging appender
	 * 
	 * @param loggerName
	 * @param filePath
	 * @param pattern 
	 * @return
	 */
	private static Logger createLogger(String loggerName, String filePath, String pattern) {
		LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
		PatternLayoutEncoder ple = new PatternLayoutEncoder();

		ple.setPattern(pattern);
		ple.setContext(lc);
		ple.start();
		FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
		fileAppender.setFile(filePath);
		fileAppender.setEncoder(ple);
		fileAppender.setContext(lc);
		fileAppender.start();

		Logger logger = (Logger) LoggerFactory.getLogger(loggerName);
		logger.addAppender(fileAppender);
		logger.setLevel(Level.ALL);
		logger.setAdditive(false); /* set to true if root should log too */

		return logger;
	}

	/**
	 * Record at INFO level
	 */
	@Override
	public void record(EventRecord record) {
		eventLogger.info(record.toString());
	}
}
