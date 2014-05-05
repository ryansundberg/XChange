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
package com.xeiam.xchange.service.streaming;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.java_websocket.WebSocket.READYSTATE;
import org.java_websocket.framing.Framedata.Opcode;
import org.java_websocket.framing.FramedataImpl1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xeiam.xchange.ExchangeException;
import com.xeiam.xchange.ExchangeSpecification;
import com.xeiam.xchange.service.BaseExchangeService;
import com.xeiam.xchange.utils.Assert;

/**
 * <p>
 * Streaming market data service to provide the following to streaming market data API:
 * </p>
 * <ul>
 * <li>Connection to an upstream market data source with a configured provider</li>
 * </ul>
 */
public abstract class BaseWebSocketExchangeService extends BaseExchangeService implements StreamingExchangeService {

  private final Logger log = LoggerFactory.getLogger(BaseWebSocketExchangeService.class);
  private final ExchangeStreamingConfiguration exchangeStreamingConfiguration;

  /**
   * The event queue for the consumer
   */
  protected final BlockingQueue<ExchangeEvent> consumerEventQueue = new LinkedBlockingQueue<ExchangeEvent>();

  protected ReconnectService reconnectService;
  private Timer timer;

  /**
   * The exchange event producer
   */
  private WebSocketEventProducer exchangeEventProducer;
  protected ExchangeEventListener exchangeEventListener;

  /**
   * Constructor
   * 
   * @param exchangeSpecification The {@link ExchangeSpecification}
   */
  protected BaseWebSocketExchangeService(ExchangeSpecification exchangeSpecification, ExchangeStreamingConfiguration exchangeStreamingConfiguration) {

    super(exchangeSpecification);
    this.exchangeStreamingConfiguration = exchangeStreamingConfiguration;
    reconnectService = new ReconnectService(this, exchangeStreamingConfiguration, reconnectErrorCallback()); // re-connect enabled by default
  }

  protected void internalConnect(URI uri, Map<String, String> headers) {
    log.debug("internalConnect to {}", uri);
    // Validate inputs
    Assert.notNull(exchangeEventListener, "exchangeEventListener cannot be null");
    
    synchronized (consumerEventQueue) { // sync on this object for connection state, since it is always present
      try {
        log.debug("Attempting to open a websocket on {}", uri);
        exchangeEventProducer = new WebSocketEventProducer(uri.toString(), exchangeEventListener, headers, reconnectService);
        exchangeEventProducer.connect();
      } catch (URISyntaxException e) {
        throw new ExchangeException("Failed to open websocket!", e);
      }
  
      if (exchangeStreamingConfiguration.keepAlive()) {
        timer = new Timer();
        timer.schedule(new KeepAliveTask(), 15000, 15000);
      }
    }
  }

  @Override
  public void disconnect() {
    log.debug("Disconnecting websocket.");
    synchronized (consumerEventQueue) {
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
      if (exchangeEventProducer != null) {
        exchangeEventProducer.close();
        exchangeEventProducer = null;
      }
    }
  }
  
  @Override
  public void start() {
    reconnectService.start();
  }
  
  @Override
  public void stop() {
    reconnectService.stop();
  }

  @Override
  public ExchangeEvent getNextEvent() throws InterruptedException {
    return consumerEventQueue.take();
  }

  public ExchangeEvent checkNextEvent() throws InterruptedException {
    if (consumerEventQueue.isEmpty()) {
      TimeUnit.MILLISECONDS.sleep(100);
    }
    return consumerEventQueue.peek();
  }

  @Override
  public void send(String msg) {
    exchangeEventProducer.send(msg);
  }

  @Override
  public READYSTATE getWebSocketStatus() {
    synchronized (consumerEventQueue) {
      if (exchangeEventProducer == null) {
        return READYSTATE.NOT_YET_CONNECTED;
      }
      else {
        return exchangeEventProducer.getConnection().getReadyState();
      }
    }
  }
  
  private Callable<Void> reconnectErrorCallback() {
    return new Callable<Void>() {
      public Void call() {
        Map<String, String> json = new HashMap<String, String>(2);
        json.put("message", "Websocket service aborted."); // failed to re-connect after n attempts
        json.put("type", "abort");
        exchangeEventListener.handleEvent(new JsonWrappedExchangeEvent(ExchangeEventType.ERROR, json));
        return null;
      }
    };
  }

  class KeepAliveTask extends TimerTask {

    @Override
    public void run() {
      synchronized (consumerEventQueue) {
        if (exchangeEventProducer != null) {
          // log.debug("Keep-Alive ping sent.");
          FramedataImpl1 frame = new FramedataImpl1(Opcode.PING);
          frame.setFin(true);
          exchangeEventProducer.getConnection().sendFrame(frame);
        }
      }
    }
  }

}
