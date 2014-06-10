package com.xeiam.xchange.atlasats.services;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.java_websocket.WebSocket.READYSTATE;
import org.eclipse.jetty.websocket.WebSocketClientFactory;
import org.cometd.client.BayeuxClient;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.bayeux.Message;
import org.cometd.websocket.client.WebSocketTransport;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.xeiam.xchange.ExchangeSpecification;
import com.xeiam.xchange.service.streaming.ExchangeStreamingConfiguration;
import com.xeiam.xchange.service.streaming.BaseWebSocketExchangeService;
import com.xeiam.xchange.service.streaming.ExchangeEvent;
import com.xeiam.xchange.service.streaming.ExchangeEventType;
import com.xeiam.xchange.service.streaming.StreamingExchangeService;
import com.xeiam.xchange.service.streaming.ReconnectService;
import com.xeiam.xchange.service.streaming.JsonWrappedExchangeEvent;
import com.xeiam.xchange.service.streaming.DefaultExchangeEvent;
import com.xeiam.xchange.atlasats.AtlasExchangeSpecification;
import com.xeiam.xchange.atlasats.AtlasStreamingConfiguration;
import com.xeiam.xchange.atlasats.dtos.streaming.marketdata.AtlasOrderBook;


public class AtlasStreamingExchangeService 
    implements StreamingExchangeService {
  
  private final Logger log = LoggerFactory.getLogger(AtlasStreamingExchangeService.class);
  
  private final BlockingQueue<ExchangeEvent> consumerEventQueue = new LinkedBlockingQueue<ExchangeEvent>();
  private final ObjectMapper streamObjectMapper;
  
  private final AtlasExchangeSpecification exchangeSpecification;
  private final AtlasStreamingConfiguration configuration;
  
  private WebSocketClientFactory wsFactory;
  private BayeuxClient client;
  private ReconnectService reconnectService;


  public AtlasStreamingExchangeService(
      ExchangeSpecification exchangeSpecification,
      ExchangeStreamingConfiguration exchangeStreamingConfiguration) {

    this.exchangeSpecification = (AtlasExchangeSpecification)exchangeSpecification;
    this.configuration = (AtlasStreamingConfiguration)exchangeStreamingConfiguration;
    wsFactory = new WebSocketClientFactory();
    try {
      wsFactory.start();
    } catch(Exception e) {
      log.error("Failure to start websocket factory", e);
    }
    client = null;
    reconnectService = new ReconnectService(this, configuration, reconnectErrorCallback());
    streamObjectMapper = new ObjectMapper();
    streamObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  @Override
  public synchronized void connect() {
    String apiUrl = configuration.isEncryptedChannel() 
        ? exchangeSpecification.getSslUriStreaming()
        : exchangeSpecification.getPlainTextUriStreaming();
    URI uri = URI.create(apiUrl);
    Map<String, Object> wsOpts = new HashMap<String, Object>();
    WebSocketTransport transport = new WebSocketTransport(wsOpts, wsFactory, null);
    log.debug("Connecting to market data stream at " + uri.toString());
    client = new BayeuxClient(uri.toString(), transport);
    client.handshake();
    client.waitFor(1000, BayeuxClient.State.CONNECTED);
    try {
      subscribeOrderData();
    }
    catch(IOException e) {
      log.error("Failed to subscribe to market data stream", e);
      disconnect();
    }
  }

  @Override
  public synchronized void disconnect() {
    if (client != null) {
      client.disconnect();
      client.waitFor(1000, BayeuxClient.State.DISCONNECTED);
      client = null;
    }
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
    // READYSTATE: NOT_YET_CONNECTED, CONNECTING, OPEN, CLOSING, CLOSED;
    if (client == null)
      return READYSTATE.CLOSED;
    
    if (client.isConnected())
      return READYSTATE.OPEN;
    
    return READYSTATE.CLOSED;
  }

  private void addToEventQueue(ExchangeEvent event) {
    reconnectService.trigger(event);
    try {
      consumerEventQueue.put(event);
    } catch (InterruptedException e) {
      log.debug("Event queue interrupted", e);
    }
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

  private void subscribeOrderData() throws IOException {
    // Subscription to channels
    ClientSessionChannel channel = client.getChannel("/market");
    channel.subscribe(new ClientSessionChannel.MessageListener()
    {
        public void onMessage(ClientSessionChannel channel, Message message)
        {
          try {
            String rawJSON = (String)message.getData();
            log.debug("JSON data received: '" + rawJSON + "'");
            ExchangeEvent orderBookEvent = new DefaultExchangeEvent(
                ExchangeEventType.SUBSCRIBE_ORDERS,
                rawJSON,
                AtlasOrderBook.createFromJson(streamObjectMapper.readTree(rawJSON)));
            addToEventQueue(orderBookEvent);
          } catch(Exception e) {
            log.error("Message handling failed", e);
          }
        }
    });
  }

}
