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

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.java_websocket.WebSocket.READYSTATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.xeiam.xchange.utils.Callback;

/**
 * @author alexnugent
 */
public class ReconnectService {

  private final Logger log = LoggerFactory.getLogger(ReconnectService.class);
  private final ExchangeStreamingConfiguration exchangeStreamingConfiguration;
  private final StreamingExchangeService streamingExchangeService;
  private final Callback errorCallback;
  private final AtomicInteger numConnectionAttempts;
  boolean running;
  Timer timer = new Timer();
  TimerTask reconnectTask;
  /**
   * Constructor with error handling
   * 
   * @param streamingExchangeService
   * @param exchangeStreamingConfiguration
   * @param errorEvent Error callback
   */
  public ReconnectService(StreamingExchangeService streamingExchangeService, ExchangeStreamingConfiguration exchangeStreamingConfiguration,
      Callback errorEvent) {

    this.streamingExchangeService = streamingExchangeService;
    this.exchangeStreamingConfiguration = exchangeStreamingConfiguration;
    this.running = true;
    this.errorCallback = errorEvent;
    this.numConnectionAttempts = new AtomicInteger(1);
  }
  
  /**
   * Constructor
   * 
   * @param streamingExchangeService
   * @param exchangeStreamingConfiguration
   */
  public ReconnectService(StreamingExchangeService streamingExchangeService, ExchangeStreamingConfiguration exchangeStreamingConfiguration)
  {
    this(streamingExchangeService, exchangeStreamingConfiguration, new Callback());
  }
  
  public void start() {
    synchronized (streamingExchangeService) {
      if(reconnectTask == null) {
        timer = new Timer();
        reconnectTask = new ReconnectTask();
        timer.schedule(reconnectTask, exchangeStreamingConfiguration.getTimeoutInMs());
      }
    }
  }
  
  /** Stop the re-connect service (stop trying to reconnect.) */
  public void stop() {
    synchronized (streamingExchangeService) {
      running = false;
      if (reconnectTask != null) {
        reconnectTask.cancel();
        reconnectTask = null;
      }
    }
  }

  public void intercept(ExchangeEvent exchangeEvent) {
    boolean isRunning;
    
    synchronized (streamingExchangeService) {
      isRunning = running;
      if (reconnectTask != null) {   
        reconnectTask.cancel();
        reconnectTask = null;
      }
      if (isRunning) {
        start(); // reschedule a check
      }
    }
    
    if(isRunning) {
      if (exchangeEvent.getEventType() == ExchangeEventType.ERROR || exchangeEvent.getEventType() == ExchangeEventType.DISCONNECT) {
        try {
          Thread.sleep(exchangeStreamingConfiguration.getReconnectWaitTimeInMs());
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        reconnect();
      }
      else if (exchangeEvent.getEventType() == ExchangeEventType.CONNECT) {
        numConnectionAttempts.set(1);
      }
    }
  }

  private void reconnect() {
    if (!streamingExchangeService.getWebSocketStatus().equals(READYSTATE.OPEN)) {
      int attempt = numConnectionAttempts.getAndIncrement();
      if (attempt > exchangeStreamingConfiguration.getMaxReconnectAttempts()) {
        log.debug("Terminating reconnection attempts.");
        streamingExchangeService.disconnect();
        errorCallback.execute();
      }
      else {
        log.debug("Attempting reconnect " + attempt + " of " + exchangeStreamingConfiguration.getMaxReconnectAttempts());
        streamingExchangeService.disconnect();
        streamingExchangeService.connect();
      }
    }
  }

  class ReconnectTask extends TimerTask {

    @Override
    public void run() {
      boolean isRunning;
      synchronized (streamingExchangeService) {
        isRunning = running;
      }
      if (isRunning) {
        // log.debug("ReconnectTask called; result: " + (streamingExchangeService.getWebSocketStatus().equals(READYSTATE.OPEN) ? "Connection still alive" : "Connection dead - reconnecting"));
        if (!streamingExchangeService.getWebSocketStatus().equals(READYSTATE.OPEN)) {
          log.debug("Websocket timed out!");
          timer.purge();
          reconnect();
        }
        timer.purge();
        start(); // reschedule a new task
      }
    }
  }

}
