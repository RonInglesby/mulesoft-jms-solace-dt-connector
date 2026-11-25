/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import static org.slf4j.LoggerFactory.getLogger;

import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapGetter;
import com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapSetter;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;

import javax.jms.Message;

import org.slf4j.Logger;

/**
 * Bridge to the Solace-native trace context of a message (the context the Solace broker reads, adds its spans
 * to, and replaces with its own span before delivery). Only used for messages of the Solace JMS API, and only if
 * the Solace OpenTelemetry JMS integration classes can be loaded; otherwise callers fall back to W3C properties.
 * All Solace classes are touched inside {@link Impl}, so this class loads even when they are missing.
 */
final class SolaceNative {

  private static final Logger LOGGER = getLogger(SolaceNative.class);
  private static final boolean AVAILABLE = detect();
  private static volatile boolean failureLogged;

  private SolaceNative() {}

  static boolean isAvailable() {
    return AVAILABLE;
  }

  /** True for messages created by the Solace JMS API. */
  static boolean isSolaceMessage(Message message) {
    return message != null && message.getClass().getName().startsWith("com.solacesystems.");
  }

  /** Solace-native context of a received message, or the root context if it has none or it can't be read. */
  static Context extract(Message message) {
    if (!AVAILABLE || !isSolaceMessage(message)) {
      return Context.root();
    }
    try {
      return Impl.extract(message);
    } catch (Throwable t) {
      logOnce("read", t);
      return Context.root();
    }
  }

  /** Writes the context into the Solace-native context of an outbound message. Never throws. */
  static void inject(Context context, Message message) {
    if (!AVAILABLE || !isSolaceMessage(message)) {
      return;
    }
    try {
      Impl.inject(context, message);
    } catch (Throwable t) {
      logOnce("write", t);
    }
  }

  private static boolean detect() {
    try {
      ClassLoader loader = SolaceNative.class.getClassLoader();
      Class.forName("com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapGetter", false, loader);
      Class.forName("com.solace.opentelemetry.javaagent.jms.SolaceJmsW3CTextMapSetter", false, loader);
      return true;
    } catch (Throwable t) {
      return false;
    }
  }

  private static void logOnce(String action, Throwable t) {
    if (!failureLogged) {
      failureLogged = true;
      LOGGER.warn("Could not {} the Solace-native trace context ({}); W3C message properties are still used. "
          + "Check that the Solace JMS API is a shared library of the app.", action, t.toString());
    } else {
      LOGGER.debug("Could not {} the Solace-native trace context", action, t);
    }
  }

  private static final class Impl {

    private static final W3CTraceContextPropagator PROPAGATOR = W3CTraceContextPropagator.getInstance();

    static Context extract(Message message) {
      return PROPAGATOR.extract(Context.root(), message, new SolaceJmsW3CTextMapGetter());
    }

    static void inject(Context context, Message message) {
      PROPAGATOR.inject(context, message, new SolaceJmsW3CTextMapSetter());
    }
  }
}
