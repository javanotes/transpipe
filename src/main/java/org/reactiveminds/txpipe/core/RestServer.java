package org.reactiveminds.txpipe.core;

import static spark.Spark.delete;
import static spark.Spark.post;
import static spark.Spark.put;
import static spark.Spark.stop;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.reactiveminds.txpipe.core.api.ServiceManager;
import org.reactiveminds.txpipe.core.dto.CreatePipeline;
import org.reactiveminds.txpipe.core.dto.HttpResponse;
import org.reactiveminds.txpipe.core.dto.TransactionResult;
import org.reactiveminds.txpipe.core.dto.HttpResponse.Code;
import org.reactiveminds.txpipe.err.DataSerializationException;
import org.reactiveminds.txpipe.err.DataValidationException;
import org.reactiveminds.txpipe.utils.JsonMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import spark.Request;
import spark.ResponseTransformer;

class RestServer {
	private static final Logger log = LoggerFactory.getLogger("RestListener");
	@Autowired
	ServiceManager manager;
	@Value("${txpipe.rest.listener.maxThreads:4}")
	private int maxThreads;
	@Value("${txpipe.rest.listener.minThreads:1}")
	private int minThreads;
	@Value("${txpipe.rest.listener.port:8081}")
	private int port;
	
	private ResponseTransformer transformer;
	@PostConstruct
	void init() {
		spark.Spark.threadPool(maxThreads, minThreads, 60000);
		spark.Spark.port(port);
		transformer = o -> JsonMapper.toString(o);
		mapEndpointUrls();
	}
	@PreDestroy
	void destroy() {
		stop();
	}
	private static String getPathParam(String param, Request req) {
		try {
			String arg = req.params(param);
			Assert.isTrue(StringUtils.hasText(arg), "param '"+param+"' missing in url path");
			return arg;
		} catch (IllegalArgumentException e) {
			throw new DataValidationException(e.getMessage());
		}
	}
	static final String PATH_PARAM_PIPELINE = ":pipeline";
	static final String PATH_PARAM_COMPONENT = ":component";
	static final String PATH_CONTEXT = "/txnpipe/";
	static final String PATH_SEP = "/";
	
	static final String URL_RUN = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "run";
	static final String URL_INVOKE = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "invoke";
	static final String URL_PIPELINE = PATH_CONTEXT + PATH_PARAM_PIPELINE;
	static final String URL_COMPONENT = URL_PIPELINE + PATH_SEP + PATH_PARAM_COMPONENT;
	
	static final String URL_PAUSE_PIPELINE = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "pause";
	static final String URL_RESUME_PIPELINE = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "resume";
	static final String URL_PAUSE_COMPONENT = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "pause" + PATH_SEP + PATH_PARAM_COMPONENT;
	static final String URL_RESUME_COMPONENT = PATH_CONTEXT + PATH_PARAM_PIPELINE + PATH_SEP + "resume" + PATH_SEP + PATH_PARAM_COMPONENT;
	
	
	@SuppressWarnings("deprecation")
	private void mapEndpointUrls() {
		/**
		 * run transaction
		 */
		post(URL_RUN, (req, res) -> {
			try {
				String componentId = getPathParam(PATH_PARAM_PIPELINE, req);
				String request = req.body();
				TransactionResult id = manager.invokePipeline(request, componentId);
				res.status(202);
				return new HttpResponse(Code.OK, "Transaction Result", id);
				
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		/**
		 * Run transaction and wait for result
		 */
		post(URL_INVOKE, (req, res) -> {
			try {
				String componentId = getPathParam(PATH_PARAM_PIPELINE, req);
				String request = req.body();
				long wait = 10;
				try {
					wait = Long.parseLong(req.queryParamOrDefault("wait", "10"));
				} catch (NumberFormatException e) {
					wait = 10;
				}
				TransactionResult id = manager.executePipeline(request, componentId, wait, TimeUnit.SECONDS);
				res.status(200);
				return new HttpResponse(Code.OK, "Transaction Result", id);
				
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (TimeoutException e) {
				log.error(e.getMessage());
				log.debug("", e);
				res.status(408);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Request Timed Out");
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		/**
		 * @deprecated
		 * configure new transaction
		 */
		put(URL_PIPELINE, (req, res) -> {
			try 
			{
				String[] components = JsonMapper.deserialize(req.body(), String[].class);
				manager.registerPipeline(getPathParam(PATH_PARAM_PIPELINE, req), components);
				log.warn("Using deprecated api to create pipeline");
				res.status(201);
				return new HttpResponse(Code.WARN, "Create Pipeline", "Using deprecated API");
				
			} catch (DataSerializationException | DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		
		/**
		 * configure new transaction
		 */
		post(URL_PIPELINE, (req, res) -> {
			try 
			{
				CreatePipeline create = JsonMapper.deserialize(req.body(), CreatePipeline.class);
				create.setName(getPathParam(PATH_PARAM_PIPELINE, req));
				manager.registerPipeline(create);
				
				res.status(201);
				return new HttpResponse(Code.OK, "Create Pipeline");
				
			} catch (DataSerializationException | DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		
		post(URL_PAUSE_PIPELINE, (req, res) -> {
			try {
				manager.pause(getPathParam(PATH_PARAM_PIPELINE, req), null);
				res.status(201);
				return new HttpResponse(Code.OK, "Pause requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		post(URL_PAUSE_COMPONENT, (req, res) -> {
			try {
				manager.pause(getPathParam(PATH_PARAM_PIPELINE, req), getPathParam(PATH_PARAM_COMPONENT, req));
				res.status(201);
				return new HttpResponse(Code.OK, "Pause requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		post(URL_RESUME_PIPELINE, (req, res) -> {
			try {
				manager.resume(getPathParam(PATH_PARAM_PIPELINE, req), null);
				res.status(201);
				return new HttpResponse(Code.OK, "Resume requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		post(URL_RESUME_COMPONENT, (req, res) -> {
			try {
				manager.resume(getPathParam(PATH_PARAM_PIPELINE, req), getPathParam(PATH_PARAM_COMPONENT, req));
				res.status(201);
				return new HttpResponse(Code.OK, "Resume requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		delete(URL_PIPELINE, (req, res) -> {
			try {
				manager.stop(getPathParam(PATH_PARAM_PIPELINE, req), null);
				res.status(201);
				return new HttpResponse(Code.OK, "Stop requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
		delete(URL_COMPONENT, (req, res) -> {
			try {
				manager.stop(getPathParam(PATH_PARAM_PIPELINE, req), getPathParam(PATH_PARAM_COMPONENT, req));
				res.status(201);
				return new HttpResponse(Code.OK, "Stop requested");
			} catch (DataValidationException e) {
				log.error("Request error> "+e.getMessage());
				log.debug("", e);
				res.status(400);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Validation Error");
				h.setDetail(e.getMessage());
				return h;
			}
			catch (Exception e) {
				log.error("Unexpected error: ", e);
				res.status(503);
				HttpResponse h = new HttpResponse();
				h.setCode(Code.ERR);
				h.setInfo("Try later. If issue persists, contact support");
				return h;
			}
		}, transformer);
	}

}
