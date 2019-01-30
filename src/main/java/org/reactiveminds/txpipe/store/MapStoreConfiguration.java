package org.reactiveminds.txpipe.store;

import org.reactiveminds.txpipe.store.MapStoreProvider.MapDBProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MapStoreConfiguration {

	@Bean
	LocalMapStoreFactory mapstoreFactory() {
		return new LocalMapStoreFactory();
	}
	@Bean
	MapStoreProvider storeProvider() {
		return new MapDBProvider();
	}

}
