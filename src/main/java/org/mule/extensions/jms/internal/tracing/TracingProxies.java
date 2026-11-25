/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import static org.slf4j.LoggerFactory.getLogger;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;

import org.apache.commons.lang3.ClassUtils;
import org.slf4j.Logger;

/**
 * Transparent JMS wrappers that add tracing without changing the connector's own send/receive code:
 * ConnectionFactory -> Connection -> Session -> MessageProducer / MessageConsumer.
 *
 * <ul>
 * <li>Every send/publish gets a PRODUCER span. Its parent is the "traceparent" user property the flow set on the
 * message (e.g. from #[attributes.traceparent]); the span's own context is then written onto the message as user
 * properties and, for Solace messages, into the Solace-native context the broker reads.</li>
 * <li>Every received message (MessageListener, receive, receiveNoWait) gets a CONSUMER span parented on the
 * Solace-native context (which the broker has replaced with its own span), else on the traceparent property. Its
 * context is recorded by JMSMessageID so the attributes builder can expose it as attributes.traceparent.</li>
 * </ul>
 *
 * The proxies implement every public interface of the wrapped object that the connector can see, including Mule
 * lifecycle interfaces (Initialisable, Startable, ...), so the runtime still initialises, starts and disposes the
 * wrapped object (e.g. the JNDI connection factory). Everything is delegated, and a tracing failure never breaks
 * messaging. XA objects are not wrapped.
 */
final class TracingProxies {

  private static final Logger LOGGER = getLogger(TracingProxies.class);

  private TracingProxies() {}

  static ConnectionFactory wrapFactory(ConnectionFactory factory, JmsTracing tracing) {
    if (factory == null || factory instanceof XAConnectionFactory || isTracingProxy(factory)) {
      if (factory instanceof XAConnectionFactory) {
        LOGGER.warn("JMS tracing is not applied to XA connection factories; messages are sent and received untraced.");
      }
      return factory;
    }
    return (ConnectionFactory) proxy(factory, new FactoryHandler(factory, tracing));
  }

  // ---------------------------------------------------------------------------------------------- plumbing

  private static Object proxy(Object target, Handler handler) {
    // The connector's class loader sees javax.jms, the Mule API (lifecycle) and Spring; vendor-internal interfaces it
    // can't see are left out, which is fine because the connector only calls the object through the visible ones.
    ClassLoader loader = TracingProxies.class.getClassLoader();
    List<Class<?>> interfaces = new ArrayList<>();
    boolean hasJmsInterface = false;
    for (Class<?> type : ClassUtils.getAllInterfaces(target.getClass())) {
      if (isUsable(type, loader)) {
        interfaces.add(type);
        hasJmsInterface |= type.getName().startsWith("javax.jms.");
      }
    }
    if (!hasJmsInterface) {
      LOGGER.warn("Could not add tracing to {} (no JMS interface visible to the connector); continuing untraced",
                  target.getClass().getName());
      return target;
    }
    try {
      return Proxy.newProxyInstance(loader, interfaces.toArray(new Class<?>[0]), handler);
    } catch (IllegalArgumentException e) {
      LOGGER.warn("Could not add tracing to {}; continuing untraced", target.getClass().getName(), e);
      return target;
    }
  }

  /** A public interface that resolves to the very same class from the given loader. */
  private static boolean isUsable(Class<?> type, ClassLoader loader) {
    if (!Modifier.isPublic(type.getModifiers())) {
      return false;
    }
    try {
      return Class.forName(type.getName(), false, loader) == type;
    } catch (Throwable t) {
      return false;
    }
  }

  private static boolean isTracingProxy(Object object) {
    return object != null && Proxy.isProxyClass(object.getClass())
        && Proxy.getInvocationHandler(object) instanceof Handler;
  }

  private static Object unwrap(Object object) {
    return isTracingProxy(object) ? ((Handler) Proxy.getInvocationHandler(object)).target : object;
  }

  private abstract static class Handler implements InvocationHandler {

    final Object target;
    final JmsTracing tracing;

    Handler(Object target, JmsTracing tracing) {
      this.target = target;
      this.tracing = tracing;
    }

    @Override
    public final Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      if (method.getDeclaringClass() == Object.class) {
        switch (method.getName()) {
          case "equals":
            return target.equals(unwrap(args[0]));
          case "hashCode":
            return target.hashCode();
          case "toString":
            return target.toString();
          default:
            break;
        }
      }
      return handle(method, args);
    }

    abstract Object handle(Method method, Object[] args) throws Throwable;

    final Object call(Method method, Object[] args) throws Throwable {
      if (args != null) {
        for (int i = 0; i < args.length; i++) {
          args[i] = unwrap(args[i]);
        }
      }
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }

  // ---------------------------------------------------------------------------------------------- handlers

  private static final class FactoryHandler extends Handler {

    FactoryHandler(Object target, JmsTracing tracing) {
      super(target, tracing);
    }

    @Override
    Object handle(Method method, Object[] args) throws Throwable {
      Object result = call(method, args);
      if (result instanceof Connection && !(result instanceof XAConnection)) {
        return proxy(result, new ConnectionHandler(result, tracing));
      }
      return result;
    }
  }

  private static final class ConnectionHandler extends Handler {

    ConnectionHandler(Object target, JmsTracing tracing) {
      super(target, tracing);
    }

    @Override
    Object handle(Method method, Object[] args) throws Throwable {
      Object result = call(method, args);
      if (result instanceof Session && !(result instanceof XASession)) {
        return proxy(result, new SessionHandler(result, tracing));
      }
      return result;
    }
  }

  private static final class SessionHandler extends Handler {

    SessionHandler(Object target, JmsTracing tracing) {
      super(target, tracing);
    }

    @Override
    Object handle(Method method, Object[] args) throws Throwable {
      Object result = call(method, args);
      if (result instanceof MessageProducer) {
        return proxy(result, new ProducerHandler(result, tracing));
      }
      if (result instanceof MessageConsumer) {
        return proxy(result, new ConsumerHandler(result, tracing));
      }
      return result;
    }
  }

  /** MessageProducer / QueueSender / TopicPublisher: traces send(...) and publish(...). */
  private static final class ProducerHandler extends Handler {

    ProducerHandler(Object target, JmsTracing tracing) {
      super(target, tracing);
    }

    @Override
    Object handle(Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if (args == null || !("send".equals(name) || "publish".equals(name))) {
        return call(method, args);
      }
      Message message = null;
      Destination destination = null;
      for (Object arg : args) {
        if (arg instanceof Message && message == null) {
          message = (Message) arg;
        } else if (arg instanceof Destination && destination == null) {
          destination = (Destination) unwrap(arg);
        }
      }
      if (message == null) {
        return call(method, args);
      }
      if (destination == null) {
        try {
          destination = ((MessageProducer) target).getDestination();
        } catch (Exception e) {
          // anonymous producer without destination: name stays unknown
        }
      }

      Span span = startSendSpan(message, destination);
      try {
        return call(method, args);
      } catch (Throwable t) {
        if (span != null) {
          span.recordException(t);
          span.setStatus(StatusCode.ERROR, String.valueOf(t.getMessage()));
        }
        throw t;
      } finally {
        if (span != null) {
          try {
            String messageId = message.getJMSMessageID();
            if (messageId != null) {
              span.setAttribute(AttributeKey.stringKey("messaging.message.id"), messageId);
            }
          } catch (Exception ignore) {
            // optional attribute
          }
          span.end();
        }
      }
    }

    /** Starts the PRODUCER span and writes its context onto the message. Returns null if tracing failed. */
    private Span startSendSpan(Message message, Destination destination) {
      try {
        String destinationName = TraceContexts.name(destination);
        Span span = tracing.tracer().spanBuilder(destinationName + " send")
            .setParent(TraceContexts.parentOfOutbound(message))
            .setSpanKind(SpanKind.PRODUCER)
            .setAttribute(AttributeKey.stringKey("messaging.system"), "jms")
            .setAttribute(AttributeKey.stringKey("messaging.operation"), "send")
            .setAttribute(AttributeKey.stringKey("messaging.destination.name"), destinationName)
            .startSpan();
        try (Scope scope = span.makeCurrent()) {
          TraceContexts.inject(Context.current(), message);
        }
        return span;
      } catch (Throwable t) {
        LOGGER.debug("Could not trace JMS send", t);
        return null;
      }
    }
  }

  /** MessageConsumer / QueueReceiver / TopicSubscriber: traces receive*() and MessageListener deliveries. */
  private static final class ConsumerHandler extends Handler {

    ConsumerHandler(Object target, JmsTracing tracing) {
      super(target, tracing);
    }

    @Override
    Object handle(Method method, Object[] args) throws Throwable {
      String name = method.getName();
      if ("setMessageListener".equals(name) && args != null && args.length == 1 && args[0] instanceof MessageListener
          && !(args[0] instanceof TracingMessageListener)) {
        args[0] = new TracingMessageListener((MessageListener) args[0], tracing);
        return call(method, args);
      }
      Object result = call(method, args);
      if (result instanceof TracingMessageListener) {
        return ((TracingMessageListener) result).delegate; // getMessageListener returns what the caller set
      }
      if (result instanceof Message && name.startsWith("receive")) {
        traceReceive((Message) result, tracing);
      }
      return result;
    }
  }

  private static final class TracingMessageListener implements MessageListener {

    private final MessageListener delegate;
    private final JmsTracing tracing;

    TracingMessageListener(MessageListener delegate, JmsTracing tracing) {
      this.delegate = delegate;
      this.tracing = tracing;
    }

    @Override
    public void onMessage(Message message) {
      traceReceive(message, tracing);
      delegate.onMessage(message);
    }
  }

  /**
   * Records a CONSUMER span for a received message and registers its context for the attributes builder.
   * The span covers the receipt only; flows continue the trace by passing attributes.traceparent onwards.
   */
  private static void traceReceive(Message message, JmsTracing tracing) {
    try {
      TraceContexts.Extracted parent = TraceContexts.parentOfInbound(message);
      String destinationName = TraceContexts.name(message.getJMSDestination());
      Span span = tracing.tracer().spanBuilder(destinationName + " receive")
          .setParent(parent.context)
          .setSpanKind(SpanKind.CONSUMER)
          .setAttribute(AttributeKey.stringKey("messaging.system"), "jms")
          .setAttribute(AttributeKey.stringKey("messaging.operation"), "receive")
          .setAttribute(AttributeKey.stringKey("messaging.destination.name"), destinationName)
          .setAttribute(AttributeKey.stringKey("trace.context.source"), parent.source)
          .startSpan();
      String messageId = message.getJMSMessageID();
      if (messageId != null) {
        span.setAttribute(AttributeKey.stringKey("messaging.message.id"), messageId);
      }
      Map<String, String> context = TraceContexts.toW3c(Context.root().with(span));
      span.end();
      ReceivedTraceRegistry.put(messageId, new ReceivedTraceRegistry.TraceInfo(context.get(TraceContexts.TRACEPARENT),
                                                                               context.get(TraceContexts.TRACESTATE),
                                                                               parent.source));
    } catch (Throwable t) {
      LOGGER.debug("Could not trace JMS receive", t);
    }
  }
}
