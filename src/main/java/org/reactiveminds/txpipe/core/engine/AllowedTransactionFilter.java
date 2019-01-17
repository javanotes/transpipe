package org.reactiveminds.txpipe.core.engine;

import java.util.function.Predicate;

interface AllowedTransactionFilter extends Predicate<String>{
	
}