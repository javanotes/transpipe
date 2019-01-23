package org.reactiveminds.txpipe.spi.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.reactiveminds.txpipe.api.AbstractTransactionService;
import org.reactiveminds.txpipe.api.CommitFailedException;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.err.ServiceDiscoveryException;
import org.reactiveminds.txpipe.err.ServiceRuntimeException;
import org.reactiveminds.txpipe.spi.DiscoveryAgent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StringUtils;

/**
 * A {@linkplain DiscoveryAgent} that can execute javascript scripts. The script should expect atleast 
 * 2 property bindings - <li><b>txnId</b></li> <li><b>type</b> (<i>commit</i>/<i>rollback</i>/<i>abort</i>)</li>. Additionally for commit types, another
 * binding <li><b>payload</b></li>' will also be present. The payload will be a request json, and the commit should optionally return a response json.
 * 
 * @author Sutanu_Dalui
 *
 */
public class JavaScriptDiscoveryAgent implements DiscoveryAgent{

	@Value("${txpipe.core.discoveryAgent.js.loadDir:ext/js}")
	private String jsDir;
	
	private class JScriptTransactionService extends AbstractTransactionService{

		private JScriptTransactionService(CompiledScript script) {
			super();
			this.script = script;
		}

		private final CompiledScript script;
		
		@Override
		public String commit(String txnId, String payload) throws CommitFailedException {
			Bindings b = nashorn.createBindings();
			b.put("txnId", txnId);
			b.put("payload", payload);
			b.put("type", "commit");
			try {
				return (String) script.eval(b);
			} catch (ScriptException e) {
				throw new ServiceRuntimeException("Script execution error", e);
			}
		}

		@Override
		public void rollback(String txnId) {
			Bindings b = nashorn.createBindings();
			b.put("txnId", txnId);
			b.put("type", "rollback");
			try {
				script.eval(b);
			} catch (ScriptException e) {
				throw new ServiceRuntimeException("Script execution error", e);
			}
		}

		@Override
		public void abort(String txnId) {
			Bindings b = nashorn.createBindings();
			b.put("txnId", txnId);
			b.put("type", "abort");
			try {
				script.eval(b);
			} catch (ScriptException e) {
				throw new ServiceRuntimeException("Script execution error", e);
			}
		}
		
	}
	private volatile Map<String, JScriptTransactionService> compiled = new HashMap<>();
	private ScriptEngine nashorn;
	public JavaScriptDiscoveryAgent() {
		nashorn = new ScriptEngineManager().getEngineByName("nashorn");
	}
	@Override
	public TransactionService getServiceById(String id) {
		if(!compiled.containsKey(id)) {
			synchronized (compiled) {
				if (!compiled.containsKey(id)) {
					loadService(id); 
				}
			}
		}
		return compiled.get(id);
	}
	private void loadService(String id) {
		try {
			File script = loadJavaScript(id);
			JScriptTransactionService service = new JScriptTransactionService(
					((Compilable) nashorn).compile(new FileReader(script)));
			compiled.put(id, service);
		} 
		catch (ScriptException | IOException e) {
			throw new ServiceDiscoveryException("Unable to load javascript module", e);
		}
	}
	private static boolean isJsFile(String name, Path file) {
		return name.equals(StringUtils.getFilename(file.getFileName().toString()))
				&& "js".equalsIgnoreCase(StringUtils.getFilenameExtension(file.getFileName().toString()));
	}
	private File loadJavaScript(String id) throws IOException {
		Path found;
		try {
			found = Files.find(ResourceUtils.getFile(jsDir).toPath(), Integer.MAX_VALUE,
					(filePath, fileAttr) -> fileAttr.isRegularFile() && isJsFile(id, filePath)).findFirst().get();
		} catch (NoSuchElementException e) {
			throw new FileNotFoundException(id);
		}
		return found.toFile();
	}
	
}