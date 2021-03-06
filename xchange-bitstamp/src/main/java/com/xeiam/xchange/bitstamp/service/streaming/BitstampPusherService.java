/**
 * Copyright (C) 2012 - 2014 Xeiam LLC http://xeiam.com
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.xeiam.xchange.bitstamp.service.streaming;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;

import javax.print.DocFlavor.STRING;

import org.java_websocket.WebSocket.READYSTATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pusher.client.Pusher;
import com.pusher.client.channel.Channel;
import com.pusher.client.channel.SubscriptionEventListener;
import com.pusher.client.connection.ConnectionEventListener;
import com.pusher.client.connection.ConnectionStateChange;
import com.pusher.client.connection.ConnectionState;
import com.xeiam.xchange.ExchangeSpecification;
import com.xeiam.xchange.bitstamp.BitstampAdapters;
import com.xeiam.xchange.bitstamp.dto.marketdata.BitstampStreamingOrderBook;
import com.xeiam.xchange.bitstamp.dto.marketdata.BitstampTransaction;
import com.xeiam.xchange.bitstamp.service.BitstampBaseService;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.dto.marketdata.OrderBook;
import com.xeiam.xchange.dto.marketdata.Trade;
import com.xeiam.xchange.service.streaming.DefaultExchangeEvent;
import com.xeiam.xchange.service.streaming.ExchangeEvent;
import com.xeiam.xchange.service.streaming.ExchangeEventType;
import com.xeiam.xchange.service.streaming.JsonWrappedExchangeEvent;
import com.xeiam.xchange.service.streaming.ReconnectService;
import com.xeiam.xchange.service.streaming.StreamingExchangeService;

/**
 * <p>
 * Streaming trade service for the Bitstamp exchange
 * </p>
 */
public class BitstampPusherService extends BitstampBaseService implements StreamingExchangeService {

  private final Logger log = LoggerFactory.getLogger(BitstampPusherService.class);

  // private final ExchangeEventListener exchangeEventListener;
  private final BlockingQueue<ExchangeEvent> consumerEventQueue = new LinkedBlockingQueue<ExchangeEvent>();
  private final ObjectMapper streamObjectMapper;

  /**
   * Ensures that exchange-specific configuration is available
   */
  private final BitstampStreamingConfiguration configuration;

  private Pusher client;
  private Map<String, Channel> channels;
  private ReconnectService reconnectService;

  /**
   * Constructor
   * 
   * @param exchangeSpecification The {@link ExchangeSpecification}
   */
  public BitstampPusherService(ExchangeSpecification exchangeSpecification, BitstampStreamingConfiguration configuration) {

    super(exchangeSpecification);

    this.configuration = configuration;
    this.client = new Pusher(configuration.getPusherKey(), configuration.pusherOptions());
    this.reconnectService = new ReconnectService(this, configuration, reconnectErrorCallback());
    this.channels = new HashMap<String, Channel>();

    streamObjectMapper = new ObjectMapper();
    streamObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public synchronized void connect() {
    client = new Pusher(configuration.getPusherKey(), configuration.pusherOptions());
    client.connect(this.connectionEventListener(), ConnectionState.ALL);
    channels.clear();
    for (String name : configuration.getChannels()) {
      Channel instance = client.subscribe(name);
      if (name == "order_book") {
        bindOrderData(instance);
      }
      else if (name == "live_trades") {
        bindTradeData(instance);
      }
      else {
        throw new IllegalArgumentException(name);
      }
      channels.put(name, instance);
    }
  }

  @Override
  public synchronized void disconnect() {
    client.disconnect();
    channels.clear();
  }
  
  @Override
  public synchronized void start() {
    reconnectService.start();
  }
  
  @Override
  public synchronized void stop() {
    reconnectService.stop();
  }

  /**
   * <p>
   * Returns next event in consumer event queue, then removes it.
   * </p>
   * 
   * @return An ExchangeEvent
   */
  @Override
  public ExchangeEvent getNextEvent() throws InterruptedException {

    return consumerEventQueue.take();
  }

  /**
   * <p>
   * Sends a msg over the socket.
   * </p>
   */
  @Override
  public void send(String msg) {

    // There's nothing to send for the current API!
  }

  /**
   * <p>
   * Query the current state of the socket.
   * </p>
   */
  @Override
  public READYSTATE getWebSocketStatus() {

    // ConnectionState: CONNECTING, CONNECTED, DISCONNECTING, DISCONNECTED, ALL
    // mapped to:
    // READYSTATE: NOT_YET_CONNECTED, CONNECTING, OPEN, CLOSING, CLOSED;
    switch (client.getConnection().getState()) {
    case CONNECTING:
      return READYSTATE.CONNECTING;

    case CONNECTED:
      return READYSTATE.OPEN;

    case DISCONNECTING:
      return READYSTATE.CLOSING;

    case DISCONNECTED:
      return READYSTATE.CLOSED;

    default:
      return READYSTATE.NOT_YET_CONNECTED;
    }
  }

  private void bindOrderData(Channel chan) {

    SubscriptionEventListener listener = new SubscriptionEventListener() {

      @Override
      public void onEvent(String channelName, String eventName, String data) {

        ExchangeEvent xevt = null;
        try {
          OrderBook snapshot = parseOrderBook(data);
          xevt = new DefaultExchangeEvent(ExchangeEventType.SUBSCRIBE_ORDERS, data, snapshot);
        } catch (IOException e) {
          log.error("JSON stream error", e);
        }
        if (xevt != null) {
          addToEventQueue(xevt);
        }
      }
    };
    chan.bind("data", listener);
  }

  private OrderBook parseOrderBook(String rawJson) throws IOException {

    BitstampStreamingOrderBook nativeBook = streamObjectMapper.readValue(rawJson, BitstampStreamingOrderBook.class);
    // BitstampOrderBook nativeBook = new BitstampOrderBook((new Date()).getTime(), json.get("bids"), json.get("asks"));
    return BitstampAdapters.adaptOrders(nativeBook, CurrencyPair.BTC_USD, 1);
  }

  private void bindTradeData(Channel chan) {

    SubscriptionEventListener listener = new SubscriptionEventListener() {

      @Override
      public void onEvent(String channelName, String eventName, String data) {

        ExchangeEvent xevt = null;
        try {
          Trade t = parseTrade(data);
          xevt = new DefaultExchangeEvent(ExchangeEventType.TRADE, data, t);
        } catch (IOException e) {
          log.error("JSON stream error", e);
        }
        if (xevt != null) {
          addToEventQueue(xevt);
        }
      }
    };
    chan.bind("trade", listener);
  }

  private Trade parseTrade(String rawJson) throws IOException {

    BitstampTransaction transaction = streamObjectMapper.readValue(rawJson, BitstampTransaction.class);
    return BitstampAdapters.adaptTrade(transaction, CurrencyPair.BTC_USD, 1);
  }

  private void addToEventQueue(ExchangeEvent event) {
    reconnectService.trigger(event);
    try {
      consumerEventQueue.put(event);
    } catch (InterruptedException e) {
      log.debug("Event queue interrupted", e);
    }
  }
  
  private ConnectionEventListener connectionEventListener() {
    return new ConnectionEventListener() {
      @Override
      public void onConnectionStateChange(ConnectionStateChange change) {
        switch(change.getCurrentState()) {
        case CONNECTED:
          log.debug("Connected to Pusher service.");
          addToEventQueue(new JsonWrappedExchangeEvent(ExchangeEventType.CONNECT, "connected"));
          break;
  
        case DISCONNECTED:
          log.debug("Disconnected from Pusher service.");
          addToEventQueue(new JsonWrappedExchangeEvent(ExchangeEventType.DISCONNECT, "disconnected"));
          break;
        }
      }

      @Override
      public void onError(String message, String code, Exception e) {
        log.debug("Connection error: " + message);
        addToEventQueue(new JsonWrappedExchangeEvent(ExchangeEventType.ERROR, message));
      }
    };
  }
  
  private Callable reconnectErrorCallback() {
    return new Callable<Void>() {
      public Void call() {
        Map<String, String> json = new HashMap<String, String>(2);
        json.put("message", "Websocket service aborted."); // failed to re-connect after n attempts
        json.put("type", "abort");
        addToEventQueue(new JsonWrappedExchangeEvent(ExchangeEventType.ERROR, json));
        return null;
      }
    };
  }

}
