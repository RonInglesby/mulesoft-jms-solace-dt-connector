/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.internal.tracing;

import static org.mule.runtime.api.meta.ExpressionSupport.NOT_SUPPORTED;

import org.mule.runtime.extension.api.annotation.Expression;
import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;
import org.mule.runtime.extension.api.annotation.param.display.DisplayName;
import org.mule.runtime.extension.api.annotation.param.display.Summary;

/**
 * OpenTelemetry settings for the Generic Connection (shown on a "Tracing" tab in Studio).
 * Tracing is off by default, so existing apps behave exactly as before.
 */
public class TracingParameters {

  @Parameter
  @Optional(defaultValue = "false")
  @Expression(NOT_SUPPORTED)
  @DisplayName("Tracing enabled")
  @Summary("Create OpenTelemetry spans for every send and receive, and propagate W3C trace context "
      + "(including the Solace-native context when the Solace JMS API is used)")
  private boolean tracingEnabled;

  @Parameter
  @Optional(defaultValue = "mule-jms")
  @Expression(NOT_SUPPORTED)
  @DisplayName("Service name")
  @Summary("service.name of the spans in Jaeger or any OTLP backend")
  private String tracingServiceName;

  @Parameter
  @Optional(defaultValue = "http://localhost:4318/v1/traces")
  @Expression(NOT_SUPPORTED)
  @DisplayName("OTLP endpoint")
  @Summary("OTLP/HTTP traces endpoint of your OpenTelemetry Collector")
  private String tracingOtlpEndpoint;

  public boolean isTracingEnabled() {
    return tracingEnabled;
  }

  public String getTracingServiceName() {
    return tracingServiceName;
  }

  public String getTracingOtlpEndpoint() {
    return tracingOtlpEndpoint;
  }
}
