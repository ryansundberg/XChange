package com.xeiam.xchange.atlasats;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.ExchangeFactory;
import com.xeiam.xchange.ExchangeSpecification;
import com.xeiam.xchange.service.polling.PollingAccountService;
import com.xeiam.xchange.service.polling.PollingMarketDataService;
import com.xeiam.xchange.service.polling.PollingTradeService;
import com.xeiam.xchange.service.streaming.StreamingExchangeService;

public class AtlasExchangeTest {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(AtlasExchangeTest.class);

	private AtlasExchange defaultExchange;
	private AtlasExchange testExchange;
	private AtlasExchangeSpecification exchangeSpecification;

	@Before
	public void setUp() throws Exception {
		defaultExchange = (AtlasExchange) ExchangeFactory.INSTANCE
				.createExchange(AtlasExchange.class.getCanonicalName());
		exchangeSpecification = new AtlasTestExchangeSpecification();
		testExchange = (AtlasExchange) ExchangeFactory.INSTANCE
				.createExchange(exchangeSpecification);
	}

	@After
	public void tearDown() throws Exception {
		defaultExchange = null;
		testExchange = null;
	}

	@Test
	public void testGetDefaultExchangeSpecification() {
		assertNotNull(defaultExchange);
		ExchangeSpecification specification = defaultExchange
				.getDefaultExchangeSpecification();
		assertNotNull(specification);
		LOGGER.info(specification.getExchangeName());
		LOGGER.info(specification.getHost());
		LOGGER.info(specification.getSslUri());
		LOGGER.info(specification.getSslUriStreaming());
	}

	@Test
	public void testTestExchange() {
		assertNotNull(testExchange);
		ExchangeSpecification specification = testExchange
				.getExchangeSpecification();
		assertNotNull(specification);
		LOGGER.info(specification.getExchangeName());
		LOGGER.info(specification.getHost());
		LOGGER.info(specification.getSslUri());
		LOGGER.info(specification.getSslUriStreaming());
	}

	@Test
	public void testApplySpecification() {
		assertNotNull(defaultExchange);
	    AtlasStreamingConfiguration streamingConfig = new AtlasStreamingConfiguration();
		PollingAccountService accountService = defaultExchange
				.getPollingAccountService();
		PollingMarketDataService marketDataService = defaultExchange
				.getPollingMarketDataService();
		PollingTradeService tradeService = defaultExchange
				.getPollingTradeService();
		StreamingExchangeService streamingService = defaultExchange.getStreamingExchangeService(streamingConfig);
		assertNull(accountService);
		assertNull(marketDataService);
		assertNull(tradeService);
		assertNull(streamingService);
		defaultExchange.applySpecification(exchangeSpecification);
		accountService = defaultExchange.getPollingAccountService();
		marketDataService = defaultExchange.getPollingMarketDataService();
		tradeService = defaultExchange.getPollingTradeService();
		streamingService = defaultExchange.getStreamingExchangeService(streamingConfig);
		assertNotNull(accountService);
		assertNotNull(marketDataService);
		assertNotNull(tradeService);
	    assertNotNull(streamingService);
	}

	@Test
	public void testGetExchangeSpecification() {
		assertNotNull(defaultExchange);
		ExchangeSpecification specification = defaultExchange
				.getExchangeSpecification();
		assertNotNull(specification);
		LOGGER.info(specification.toString());
	}

}
