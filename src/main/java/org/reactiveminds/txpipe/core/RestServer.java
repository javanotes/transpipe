package org.reactiveminds.txpipe.core;

import static spark.Spark.post;
import static spark.Spark.put;
import static spark.Spark.stop;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.core.api.ComponentManager;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

class RestServer {
	private static final Logger log = LoggerFactory.getLogger(RestServer.class);
	@Autowired
	ComponentManager manager;
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
		/*
		 * run transaction
		 */
		post("/txnpipe/:pipeline/run", (req, res) -> {
			try {
				String componentId = req.params(":pipeline");
				String request = req.body();
				String id = manager.invokePipeline(request, componentId);
				res.status(201);
				return id;
			} catch (IllegalArgumentException e) {
				log.warn("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(500);
				return "Server Error!";
			}
		});
		/*
		 * configure new transaction
		 */
		put("/txnpipe/:pipeline", (req, res) -> {
			try {
				String[] components = new JsonMapper().toObject(req.body(), String[].class);
				PipelineDef request = new PipelineDef();
				request.setPipelineId(req.params(":pipeline"));
				for(String component : components) {
					request.addComponent(component);
				}
				manager.registerPipeline(request);
				res.status(201);
				return "OK";
			} catch (IllegalArgumentException e) {
				log.warn("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				return e.getMessage();
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(500);
				return "Server Error!";
			}
		});
		
	}

}
