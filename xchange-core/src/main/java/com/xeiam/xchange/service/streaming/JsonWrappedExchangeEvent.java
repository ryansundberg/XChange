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

import java.util.Map;
import java.util.HashMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;

/**
 * <p>
 * Exchange event that provides convenience constructors for JSON wrapping
 * </p>
 */
public class JsonWrappedExchangeEvent extends DefaultExchangeEvent {

  /**
   * @param exchangeEventType The exchange event type
   * @param json Event data to become JSON
   */
  public JsonWrappedExchangeEvent(ExchangeEventType exchangeEventType, Object json) {
    super(exchangeEventType);
    data = buildJSON(payload);
    payload = json;
  }
  
  /**
   * @param exchangeEventType The exchange event type
   * @param message The message content without JSON wrapping (will get a {"message":"parameter value"} wrapping)
   */
  public JsonWrappedExchangeEvent(ExchangeEventType exchangeEventType, String message) {
    super(exchangeEventType);
    HashMap<String, String> json = new HashMap<String, String>(1);
    json.put("message", message);
    data = buildJSON(payload);
    payload = json;
  }  
  
  private static String buildJSON(Object obj) {
    try {
      return (new ObjectMapper()).writeValueAsString(obj);
    } catch (JsonProcessingException err) {
      return "undefined";
    }
  }

}
