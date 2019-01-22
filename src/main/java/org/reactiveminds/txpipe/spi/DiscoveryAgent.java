package org.reactiveminds.txpipe.spi;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.reactiveminds.txpipe.PlatformConfiguration;
import org.reactiveminds.txpipe.api.AbstractTransactionService;
import org.reactiveminds.txpipe.api.TransactionService;
import org.reactiveminds.txpipe.err.CommitFailedException;
import org.springframework.util.ResourceUtils;

public interface DiscoveryAgent {
	String JS_AGENT_DIR = "txpipe.core.discoveryAgent.js.loadDir";
	/**
	 * Get a service instance, singleton probably, with given id. This id is the name used in pipeline
	 * definition.
	 * @param id
	 * @return
	 */
	TransactionService getServiceById(String id);
	/**
	 * The default {@linkplain DiscoveryAgent}. Expects the service to be present in Spring context. Non Spring applications
	 * can provide a different agent to load service class.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class SpringContextDiscoveryAgent implements DiscoveryAgent{

		@Override
		public TransactionService getServiceById(String id) {
			return PlatformConfiguration.getMangedBeanOfName(id, TransactionService.class);
		}
		
	}
	/**
	 * A {@linkplain DiscoveryAgent} that can execute javascript scripts. The script should expect atleast 
	 * 2 property bindings - 'txnId' and 'type' (commit/rollback/abort). Additionally for commit types, another
	 * binding 'payload' will also be present.
	 * @author Sutanu_Dalui
	 *
	 */
	public static class JScriptDiscoveryAgent implements DiscoveryAgent{

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
					throw new RuntimeException("Script execution exception", e);
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
					throw new RuntimeException("Script execution exception", e);
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
					throw new RuntimeException("Script execution exception", e);
				}
			}
			
		}
		private ConcurrentMap<String, JScriptTransactionService> compiled = new ConcurrentHashMap<>();
		private ScriptEngine nashorn;
		public JScriptDiscoveryAgent() {
			nashorn = new ScriptEngineManager().getEngineByName("nashorn");
		}
		@Override
		public TransactionService getServiceById(String id) {
			if(!compiled.containsKey(id)) {
				try {
					File script = loadJavascript(id);
					JScriptTransactionService service = new JScriptTransactionService(((Compilable)nashorn).compile(new FileReader(script)));
					compiled.putIfAbsent(id, service);
				} catch (FileNotFoundException | ScriptException e) {
					throw new IllegalArgumentException("Unable to load javascript module", e);
				}
			}
			return compiled.get(id);
		}
		private File loadJavascript(String id) throws FileNotFoundException {
			String dir = PlatformConfiguration.getApplicationProperty(JS_AGENT_DIR, ResourceUtils.CLASSPATH_URL_PREFIX+"ext/js");
			return ResourceUtils.getFile(dir+File.separator+id+".js");
		}
		
	}
}
