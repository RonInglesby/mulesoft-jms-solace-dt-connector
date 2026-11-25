/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;

/**
 * W3C trace-context helpers. Outbound messages carry the context twice: as "traceparent"/"tracestate" user
 * properties (readable by any JMS consumer, and by Mule flows via attributes) and, for Solace messages, in the
 * Solace-native context the broker uses. Inbound messages are parented on the Solace-native context first,
 * because the broker replaces it with its own span; the user properties are the fallback.
 */
public final class TraceContexts {

  public static final String TRACEPARENT = "traceparent";
  public static final String TRACESTATE = "tracestate";

  public static final String SOURCE_SOLACE = "solace";
  public static final String SOURCE_USER_PROPERTY = "user-property";
  public static final String SOURCE_NONE = "none";

  private static final W3CTraceContextPropagator PROPAGATOR = W3CTraceContextPropagator.getInstance();

  private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<Map<String, String>>() {

    @Override
    public Iterable<String> keys(Map<String, String> carrier) {
      return carrier.keySet();
    }

    @Override
    public String get(Map<String, String> carrier, String key) {
      return carrier == null ? null : carrier.get(key);
    }
  };

  private static final TextMapSetter<Map<String, String>> MAP_SETTER = new TextMapSetter<Map<String, String>>() {

    @Override
    public void set(Map<String, String> carrier, String key, String value) {
      if (carrier != null) {
        carrier.put(key, value);
      }
    }
  };

  /** Parent context and where it came from. */
  static final class Extracted {

    final Context context;
    final String source;

    Extracted(Context context, String source) {
      this.context = context;
      this.source = source;
    }
  }

  private TraceContexts() {}

  /** Context from W3C strings; the root context when none are given. */
  public static Context fromW3c(String traceparent, String tracestate) {
    Map<String, String> carrier = new HashMap<>();
    if (traceparent != null && !traceparent.isEmpty()) {
      carrier.put(TRACEPARENT, traceparent);
    }
    if (tracestate != null && !tracestate.isEmpty()) {
      carrier.put(TRACESTATE, tracestate);
    }
    return PROPAGATOR.extract(Context.root(), carrier, MAP_GETTER);
  }

  /** W3C strings for a context. */
  public static Map<String, String> toW3c(Context context) {
    Map<String, String> carrier = new HashMap<>();
    PROPAGATOR.inject(context, carrier, MAP_SETTER);
    return carrier;
  }

  /** Parent for a span about to be sent: the traceparent the flow put in the message's user properties. */
  static Context parentOfOutbound(Message message) {
    return fromW3c(stringProperty(message, TRACEPARENT), stringProperty(message, TRACESTATE));
  }

  /** Parent for a received message: Solace-native context, else the traceparent user property. */
  static Extracted parentOfInbound(Message message) {
    Context solace = SolaceNative.extract(message);
    if (isValid(solace)) {
      return new Extracted(solace, SOURCE_SOLACE);
    }
    Context properties = fromW3c(stringProperty(message, TRACEPARENT), stringProperty(message, TRACESTATE));
    return new Extracted(properties, isValid(properties) ? SOURCE_USER_PROPERTY : SOURCE_NONE);
  }

  /** Writes the context onto an outbound message, both ways. Never throws. */
  static void inject(Context context, Message message) {
    for (Map.Entry<String, String> entry : toW3c(context).entrySet()) {
      try {
        message.setStringProperty(entry.getKey(), entry.getValue());
      } catch (JMSException | RuntimeException e) {
        // Read-only message (e.g. a forwarded received message): the Solace-native context still carries it.
      }
    }
    SolaceNative.inject(context, message);
  }

  static boolean isValid(Context context) {
    return Span.fromContext(context).getSpanContext().isValid();
  }

  static String name(Destination destination) {
    if (destination == null) {
      return "unknown";
    }
    try {
      if (destination instanceof Queue) {
        return ((Queue) destination).getQueueName();
      }
      if (destination instanceof Topic) {
        return ((Topic) destination).getTopicName();
      }
    } catch (JMSException e) {
      // fall through
    }
    return destination.toString();
  }

  private static String stringProperty(Message message, String name) {
    try {
      Object value = message.getObjectProperty(name);
      return value == null ? null : value.toString();
    } catch (JMSException | RuntimeException e) {
      return null;
    }
  }
}
