package com.xeiam.xchange.atlasats.dtos.streaming.marketdata;

import java.math.BigDecimal;
import java.util.Date;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.JsonNode;

import com.xeiam.xchange.ExchangeException;
import com.xeiam.xchange.currency.CurrencyPair;
import com.xeiam.xchange.dto.marketdata.OrderBook;
import com.xeiam.xchange.dto.trade.LimitOrder;
import com.xeiam.xchange.dto.Order;

public class AtlasOrderBook {
  
  public static OrderBook createFromJson(JsonNode json) {
    JsonNode quotes = json.get("quotes");
    List<LimitOrder> asks = new ArrayList<LimitOrder>();
    List<LimitOrder> bids = new ArrayList<LimitOrder>();
    Date timestamp = new Date();
    CurrencyPair ccyPair = new CurrencyPair(json.get("symbol").textValue(),
        json.get("currency").textValue());
    
    for(JsonNode orderNode : quotes) {
      LimitOrder order = createLimitOrderFromJson(orderNode, timestamp, ccyPair);
      switch(order.getType()) {
      case ASK:
        asks.add(order);
        break;
      case BID:
        bids.add(order);
        break;
      }
    }
    return new OrderBook(timestamp, asks, bids);
  }
  
  private static LimitOrder createLimitOrderFromJson(JsonNode orderNode, Date timestamp, CurrencyPair ccyPair) 
    throws ExchangeException {
    //LimitOrder(OrderType type, BigDecimal tradableAmount, CurrencyPair currencyPair, String id, Date timestamp, BigDecimal limitPrice) {
    String side = orderNode.get("side").textValue();
    Order.OrderType orderType = Order.OrderType.ASK;
    if("BUY".equals(side)) {
      orderType = Order.OrderType.BID;
    }
    else if("SELL".equals(side)) {
      orderType = Order.OrderType.ASK;
    }
    else {
      throw new ExchangeException("Invalid order side: '" + side + "'");
    }
    BigDecimal quantity = orderNode.get("size").decimalValue();
    BigDecimal price = orderNode.get("price").decimalValue();
    String orderId = orderNode.get("id").textValue();
    return new LimitOrder(orderType, quantity, ccyPair, orderId, timestamp, price);
  }

}


/*

{
"openinterest":2.337,
"symbol":"BTC",
"last":250,
"change":0,
"bidsize":1.337,
"asksize":97.663,
"currency":"USD",
"id":"0",
"open":250,
"quotes":[
   {
      "id":"ATLAS:B246",
      "mm":"ATLAS",
      "price":246,
      "symbol":"BTC",
      "side":"BUY",
      "size":1.337,
      "currency":"USD"
   },
   ...
   {
      "id":"ATLAS:S400",
      "mm":"ATLAS",
      "price":400,
      "symbo23l":"BTC",
      "side":"SELL",
      "size":1000,
      "currency":"USD"
   }
],
"sequence":18,
"volume":2.34,
"high":1100,
"ask":250,
"low":201.5,
"bid":246,
"average":250
}

*/