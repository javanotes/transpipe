package com.transaction.b2b;

import org.reactiveminds.txpipe.PlatformConfiguration;
import org.reactiveminds.txpipe.api.AbstractTransactionService;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.api.TransactionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * This class is an example of how the configuration class for an integration project would
 * look like.
 * @author Sutanu_Dalui
 *
 */
@Import(PlatformConfiguration.class)
@Configuration
public class AppConfiguration {
	static final Logger log = LoggerFactory.getLogger("Service_Log");
	/*@Bean
	TransactionService checkCredit() {
		return new CreditCheckingService();
	}*/
	
	@Bean
	TransactionService bookTicket() {
		return new AbstractTransactionService() {

			@Override
			public void rollback(String txnId) {
				log.warn(txnId + " -- ticket booking rolled back --");
			}

			@Override
			public void abort(String txnId) {
				log.warn(txnId + " --  ticket booking aborted --");
			}

			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				log.info(txnId + " -- ticket booking success --");
				return "";
			}
		};
	}
	
	
	/*@Bean
	TransactionService notifyUser() {
		return new AbstractTransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn(txnId+" -- user notify rolled back --");
			}
			
			@Override
			public void abort(String txnId) {
				log.warn(txnId + " --   user notify aborted --");
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
		return new AbstractTransactionService() {
			
			@Override
			public void rollback(String txnId) {
				log.warn("-- user notify rolled back --");
			}
			
			@Override
			public String commit(String txnId, String payload) throws CommitFailedException {
				throw new CommitFailedException("failed to notify", new RuntimeException());
			}
		};
	}*/
}
