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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.java_websocket.WebSocket.READYSTATE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author alexnugent
 */
public class ReconnectService {

  private final Logger log = LoggerFactory.getLogger(ReconnectService.class);
  private final ExchangeStreamingConfiguration exchangeStreamingConfiguration;
  private final StreamingExchangeService streamingExchangeService;
  private final Callable<Void> errorCallback;
  private final AtomicBoolean go;
  private final BlockingQueue<ExchangeEvent> eventQueue = new LinkedBlockingQueue<ExchangeEvent>();
  private Thread thread = null;
  
  /**
   * Constructor with error handling
   * 
   * @param streamingExchangeService
   * @param exchangeStreamingConfiguration
   * @param errorHandler Error callback
   */
  public ReconnectService(StreamingExchangeService streamingExchangeService, ExchangeStreamingConfiguration exchangeStreamingConfiguration,
      Callable<Void> errorHandler) {

    this.streamingExchangeService = streamingExchangeService;
    this.exchangeStreamingConfiguration = exchangeStreamingConfiguration;
    this.errorCallback = errorHandler;
    this.go = new AtomicBoolean(false);
  }
  
  /**
   * Constructor
   * 
   * @param streamingExchangeService
   * @param exchangeStreamingConfiguration
   */
  public ReconnectService(StreamingExchangeService streamingExchangeService, ExchangeStreamingConfiguration exchangeStreamingConfiguration)
  {
    this(streamingExchangeService, exchangeStreamingConfiguration, null);
  }
  
  public void start() {
    synchronized (streamingExchangeService) {
      if(thread == null) {
        go.set(true);
        eventQueue.clear();
        try {
          eventQueue.put(new DefaultExchangeEvent(ExchangeEventType.DISCONNECT)); // kickstart
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
          log.error("Reconnect service start interrupted: {}", e.getMessage());
        }
        thread = new Thread(new ReconnectThread());
        thread.start();
      }
    }
  }
  
  /** Stop the re-connect service (stop trying to reconnect.) */
  public void stop() {
    synchronized (streamingExchangeService) {
      if (thread != null) {
        go.set(false);
        streamingExchangeService.disconnect();
        try {
          eventQueue.put(new DefaultExchangeEvent(ExchangeEventType.EVENT)); // unblock the Q
          thread.join();
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
          log.error("Reconnect service stop interrupted: {}", e.getMessage());
        }
        thread = null;
      }
    }
  }

  public void trigger(ExchangeEvent exchangeEvent) {
    try {
      eventQueue.put(exchangeEvent);
    }
    catch (InterruptedException e) {
      e.printStackTrace(System.err);
      log.error("Reconnect service trigger interrupted: {}", e.getMessage());
    }
  }
  
  private class ReconnectThread implements Runnable {
    
    private int attempt;
    
    public void run() {
      attempt = 0;
      
      while (go.get()) {
        try {
          ExchangeEvent evt = eventQueue.take();
          switch(evt.getEventType()) {
          case CONNECT:
            attempt = 0;
            break;
          case DISCONNECT:
          case ERROR:
          //case EVENT:
            checkConnection();
            break;
          default:
            break;
          }        
        } catch (InterruptedException e) {
          e.printStackTrace(System.err);
          log.debug("Reconnect service interrupted: {}", e.getMessage());
        }
      }
      
      if (attempt >= exchangeStreamingConfiguration.getMaxReconnectAttempts()) {
        log.debug("Aborting reconnection attempts.");
        streamingExchangeService.disconnect();
        if(errorCallback != null) {
          try {
            errorCallback.call();
          } catch (Exception e) {
            e.printStackTrace(System.err);
            log.error("Reconnect error callback exception: {}", e.getMessage());
          }
        }        
      }      
    }
    
    private void checkConnection() {
      if(!go.get()) {
        return;
      }
      try {
        switch(streamingExchangeService.getWebSocketStatus()) {
        case NOT_YET_CONNECTED:
        case CLOSED:
          if(attempt < exchangeStreamingConfiguration.getMaxReconnectAttempts()) {
            if(attempt++ > 0) {
              streamingExchangeService.disconnect();
              Thread.sleep(exchangeStreamingConfiguration.getReconnectWaitTimeInMs());
            }
            log.debug("Attempting to connect {} of {}.", attempt, exchangeStreamingConfiguration.getMaxReconnectAttempts());
            try {
              streamingExchangeService.connect();
            }
            catch (Exception e) {
              log.debug("Connection attempt {} failed: {}", attempt, e.getMessage());
            }
            // eventQueue.put(new DefaultExchangeEvent(ExchangeEventType.EVENT));
          }
          else {
            go.set(false);
          }
          break;
          
        /*case CONNECTING:
        case CLOSING:
          // check again in 10 ms
          Thread.sleep(10);
          eventQueue.put(new DefaultExchangeEvent(ExchangeEventType.EVENT));
          break;*/
        }
      } catch (InterruptedException e) {
        e.printStackTrace(System.err);
        log.error("checkConnection interrupted: {}", e.getMessage());
      }
    }
    
  }

}
