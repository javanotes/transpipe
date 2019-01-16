package org.reactiveminds.txpipe.core;

import static spark.Spark.post;
import static spark.Spark.put;
import static spark.Spark.stop;
import static spark.Spark.delete;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

class RestServer {
	private static final Logger log = LoggerFactory.getLogger("RestListener");
	@Autowired
	ServiceManager manager;
	@Value("${txpipe.rest.maxThreads:4}")
	private int maxThreads;
	@Value("${txpipe.rest.minThreads:1}")
	private int minThreads;
	@Value("${txpipe.rest.port:8081}")
	private int port;
	@PostConstruct
	void init() {
		spark.Spark.threadPool(maxThreads, minThreads, 60000);
		spark.Spark.port(port);
		mapEndpointUrls();
	}
	@PreDestroy
	void destroy() {
		stop();
	}
	private void mapEndpointUrls() {
		/**
		 * run transaction
		 */
		post("/txnpipe/:pipeline/run", (req, res) -> {
			try {
				String componentId = req.params(":pipeline");
				String request = req.body();
				String id = manager.invokePipeline(request, componentId);
				res.status(202);
				return id;
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		/**
		 * Run transaction and wait for result
		 */
		post("/txnpipe/:pipeline/invoke", (req, res) -> {
			try {
				String componentId = req.params(":pipeline");
				String request = req.body();
				long wait = 10;
				try {
					wait = Long.parseLong(req.queryParamOrDefault("wait", "10"));
				} catch (NumberFormatException e) {
					wait = 10;
				}
				String id = manager.executePipeline(request, componentId, wait, TimeUnit.SECONDS);
				res.status(200);
				return id;
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (TimeoutException e) {
				log.error(e.getMessage());
				log.debug("", e);
				res.status(408);
				return "Request timed out";
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		/**
		 * configure new transaction
		 */
		put("/txnpipe/:pipeline", (req, res) -> {
			try {
				String[] components = JsonMapper.deserialize(req.body(), String[].class);
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline");
				manager.registerPipeline(req.params(":pipeline"), components);
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		post("/txnpipe/:pipeline/pause", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline/pause");
				manager.pause(req.params(":pipeline"), null);
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		post("/txnpipe/:pipeline/pause/:txn", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline/pause");
				manager.pause(req.params(":pipeline"), req.params(":txn"));
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		post("/txnpipe/:pipeline/resume", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline/resume");
				manager.resume(req.params(":pipeline"), null);
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		post("/txnpipe/:pipeline/resume/:txn", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline/resume");
				manager.resume(req.params(":pipeline"), req.params(":txn"));
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		delete("/txnpipe/:pipeline", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline");
				manager.stop(req.params(":pipeline"), null);
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
		delete("/txnpipe/:pipeline/:txn", (req, res) -> {
			try {
				Assert.isTrue(StringUtils.hasText(req.params(":pipeline")), "pipeline id not specified in url path /txnpipe/:pipeline/:txn");
				manager.stop(req.params(":pipeline"), req.params(":txn"));
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				return "Try later. If issue persists, contact support";
			}
		});
	}

}
