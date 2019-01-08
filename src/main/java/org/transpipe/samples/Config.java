package org.transpipe.samples;

import org.reactiveminds.txpipe.api.ClientConfiguration;
import org.reactiveminds.txpipe.api.EventRecord;
import org.reactiveminds.txpipe.api.EventRecorder;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.api.EventRecorder.CommitEventRecorder;
import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.err.CommitFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ClientConfiguration.class)
public class Config {

	private static final Logger log = LoggerFactory.getLogger("TXNLOGGER");
	@Bean
	TransactionService checkCredit() {
		return new TransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn(txnId+" -- credit checking rolled back --");
			}
			
			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				log.info(txnId+" -- credit checking success --");
				return "";
			}
		};
	}
	
	@Bean
	TransactionService bookTicket() {
		return new TransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn(txnId+" -- ticket booking rolled back --");
			}
			
			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				log.info(txnId+" -- ticket booking success --");
				return "";
			}
		};
	}
	
	@Bean
	TransactionService notifyUser() {
		return new TransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn(txnId+" -- user notify rolled back --");
			}
			
			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				log.info(txnId+" -- user notify success --");
				return "";
			}
		};
	}
	
	@Bean
	TransactionService notifyUserFailed() {
		return new TransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn("-- user notify rolled back --");
			}
			
			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				throw new CommitFailedException("failed to notify", new RuntimeException());
			}
		};
	}
	
	@Bean(ServiceManager.COMMIT_RECORDER_BEAN_NAME)
	EventRecorder commitRecorder() {
		return new CommitEventRecorder() {
			
			@Override
			protected void onRecord(EventRecord record) {
				eventLogger.info("Local implementation : " + record);
			}
		};
	}
}
