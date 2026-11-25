/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Hands the receive span's context from the JMS consumer (where the raw message is available) to the attributes
 * builder (which only sees headers and properties), keyed by JMSMessageID. Entries are taken once; anything left
 * over expires, so the registry can't grow without bound.
 */
public final class ReceivedTraceRegistry {

  /** Trace context of a receive span. */
  public static final class TraceInfo {

    private final String traceparent;
    private final String tracestate;
    private final String source;

    TraceInfo(String traceparent, String tracestate, String source) {
      this.traceparent = traceparent;
      this.tracestate = tracestate;
      this.source = source;
    }

    public String getTraceparent() {
      return traceparent;
    }

    public String getTracestate() {
      return tracestate;
    }

    public String getSource() {
      return source;
    }
  }

  private static final Cache<String, TraceInfo> ENTRIES = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build();

  private ReceivedTraceRegistry() {}

  static void put(String messageId, TraceInfo info) {
    if (messageId != null && info != null) {
      ENTRIES.put(messageId, info);
    }
  }

  /** Removes and returns the context recorded for the message, or null. */
  public static TraceInfo take(String messageId) {
    if (messageId == null) {
      return null;
    }
    TraceInfo info = ENTRIES.getIfPresent(messageId);
    if (info != null) {
      ENTRIES.invalidate(messageId);
    }
    return info;
  }
}
