package org.reactiveminds.txpipe.core;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Observable;
import java.util.Observer;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.api.EventRecord;
import org.reactiveminds.txpipe.api.EventRecorder;
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
/**
 * A default implementation of {@linkplain EventRecorder} that logs to file.
 * @author Sutanu_Dalui
 *
 */
class DefaultEventRecorder implements EventRecorder {

	private static String folderName() {
		return new SimpleDateFormat("yyyyMMddHH").format(new Date());
	}
	@Value("${txpipe.event.logDir:./logs/events/}")
	private String loggingDir;
	private volatile Logger logger;
	
	private TimeChangeNotifier notifier;
	@PostConstruct
	private void onInit() {
		configureFileLogger();
		notifier = new HourlyNotifier();
		notifier.addObserver(new Observer() {
			
			@Override
			public void update(Observable o, Object arg) {
				configureFileLogger();
			}
		});
		notifier.start();
		
	}
	@PreDestroy
	private void onStop() {
		notifier.stop();
	}
	private void configureFileLogger() {
	    /*LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        
        PatternLayoutEncoder ple = new PatternLayoutEncoder();
        ple.setPattern("%date [%thread] %msg%n");
        ple.setContext(context);
        ple.start();
        
        
        RollingFileAppender<ILoggingEvent> fileAppender = new RollingFileAppender<>();
        fileAppender.setFile(loggingDir+eventLogger.getName()+".log");
        fileAppender.setEncoder(ple);
        fileAppender.setContext(context);
        
        TimeBasedRollingPolicy<ILoggingEvent> policy = new TimeBasedRollingPolicy<>();
        policy.setContext(context);
        policy.setMaxHistory(24);
        policy.setFileNamePattern(eventLogger.getName() + "-%d{yyyy-MM-dd-HH}-%i.log.gz");
        
        fileAppender.setRollingPolicy(policy);
        
        policy.setParent(fileAppender);
        policy.start();
        
        fileAppender.start();
        
        AsyncAppenderBase<ILoggingEvent> asyncLogger = new AsyncAppenderBase<>();
        asyncLogger.setContext(context);
        asyncLogger.addAppender(fileAppender);
        asyncLogger.start();
        
        logger = (Logger) LoggerFactory.getLogger("LoggingEventRecorder"); 
        logger.setAdditive(false);
        logger.addAppender(fileAppender); 
        logger.setLevel(Level.ALL);*/
        
        
        logger = createLoggerFor("LoggingEventRecorder", loggingDir + folderName() + "/" + eventLogger.getName()+".log");
	}
	
	private static Logger createLoggerFor(String string, String file) {
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        PatternLayoutEncoder ple = new PatternLayoutEncoder();

        ple.setPattern("%date [%thread] %msg%n");
        ple.setContext(lc);
        ple.start();
        FileAppender<ILoggingEvent> fileAppender = new FileAppender<ILoggingEvent>();
        fileAppender.setFile(file);
        fileAppender.setEncoder(ple);
        fileAppender.setContext(lc);
        fileAppender.start();

        Logger logger = (Logger) LoggerFactory.getLogger(string);
        logger.addAppender(fileAppender);
        logger.setLevel(Level.DEBUG);
        logger.setAdditive(false); /* set to true if root should log too */

        return logger;
  }

	@Override
	public void record(EventRecord record) {
		logger.info(record.toString());
	}
}
