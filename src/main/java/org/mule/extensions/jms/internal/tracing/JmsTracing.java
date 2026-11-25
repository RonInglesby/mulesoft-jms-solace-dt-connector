/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import static org.slf4j.LoggerFactory.getLogger;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

import java.util.concurrent.TimeUnit;

import javax.jms.ConnectionFactory;

import org.slf4j.Logger;

/**
 * Tracing for one connection provider: owns its tracer (exported over OTLP/HTTP) and wraps the provider's
 * {@link ConnectionFactory} so every send and receive is traced. When tracing is disabled nothing is wrapped.
 */
public final class JmsTracing {

  private static final Logger LOGGER = getLogger(JmsTracing.class);
  private static final String INSTRUMENTATION_NAME = "mule-jms-connector-solace-tracing";

  private final boolean enabled;
  private final String serviceName;
  private final String otlpEndpoint;

  private volatile SdkTracerProvider provider;
  private volatile Tracer tracer;

  public JmsTracing(TracingParameters parameters) {
    this.enabled = parameters != null && parameters.isTracingEnabled();
    this.serviceName = parameters == null || isBlank(parameters.getTracingServiceName())
        ? "mule-jms" : parameters.getTracingServiceName().trim();
    this.otlpEndpoint = parameters == null || isBlank(parameters.getTracingOtlpEndpoint())
        ? "http://localhost:4318/v1/traces" : parameters.getTracingOtlpEndpoint().trim();
    if (enabled) {
      LOGGER.info("JMS tracing enabled: service.name={}, OTLP endpoint={}, Solace-native context {}",
                  serviceName, otlpEndpoint, SolaceNative.isAvailable() ? "available" : "NOT available (W3C properties only)");
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  /** Returns a tracing wrapper of the factory, or the factory itself when tracing is disabled. */
  public ConnectionFactory wrap(ConnectionFactory connectionFactory) {
    return enabled ? TracingProxies.wrapFactory(connectionFactory, this) : connectionFactory;
  }

  Tracer tracer() {
    Tracer current = tracer;
    if (current != null) {
      return current;
    }
    synchronized (this) {
      if (tracer == null) {
        OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
            .setEndpoint(otlpEndpoint)
            .setTimeout(10, TimeUnit.SECONDS)
            .build();
        provider = SdkTracerProvider.builder()
            .setResource(Resource.getDefault().merge(Resource.create(
                                                                     Attributes.of(AttributeKey.stringKey("service.name"),
                                                                                   serviceName))))
            .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
            .build();
        // Not registered globally, so it can't clash with the Mule runtime's own OpenTelemetry.
        tracer = OpenTelemetrySdk.builder().setTracerProvider(provider).build().getTracer(INSTRUMENTATION_NAME);
      }
      return tracer;
    }
  }

  /** Flushes pending spans and stops the exporter. */
  public void shutdown() {
    SdkTracerProvider current = provider;
    if (current != null) {
      current.shutdown().join(5, TimeUnit.SECONDS);
      provider = null;
      tracer = null;
    }
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
