package com.xeiam.xchange.atlasats.dtos;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.Currency;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

import com.xeiam.xchange.dto.marketdata.OrderBook;
import com.xeiam.xchange.atlasats.dtos.streaming.marketdata.AtlasOrderBook;

public class AtlasOrderBookTest {
	private static final Logger LOGGER = LoggerFactory
			.getLogger(AtlasOrderBookTest.class);
	
	private InputStream inputStream;
	
	@Before
    public void setUp() {
	  inputStream = null;
	}
	
    @After
    public void tearDown() throws IOException {
      if(inputStream != null) {
        inputStream.close();
        inputStream = null;
      }
    }

	@Test
	public void testParseOrderBook() throws Exception {
	  inputStream = AtlasOrderBookTest.class.getResourceAsStream("/marketdata/orderbook.json");
	  if (inputStream == null) {
	    LOGGER.error("Could not load test data.");
	  }
	  ObjectMapper mapper = new ObjectMapper();
	  mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
	  JsonNode orderBookData = mapper.readTree(inputStream);
	  assertNotNull(orderBookData);
	  OrderBook book = AtlasOrderBook.createFromJson(orderBookData);
	  assertNotNull(book);
	  assertNotNull(book.getTimeStamp());
	}


}
