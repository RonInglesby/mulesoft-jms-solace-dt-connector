/*
 * Copyright 2023 Salesforce, Inc. All rights reserved.
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */
package org.mule.extensions.jms.api.message;

import org.mule.runtime.extension.api.annotation.param.Optional;
import org.mule.runtime.extension.api.annotation.param.Parameter;

import javax.jms.Message;

/**
 * Contains all the metadata of a JMS {@link Message}, it carries information such as the Headers,
 * the Properties and the required ID for performing an ACK on the Message.
 * <p>
 *
 * @since 1.0
 */
public class JmsAttributes implements org.mule.jms.commons.api.message.JmsAttributes {

  private static final long serialVersionUID = -8148917084189760450L;

  /**
   * Container element for all the properties present in a JMS Message
   */
  @Parameter
  private final JmsMessageProperties properties;

  /**
   * All the possible headers of a JMS Message
   */
  @Parameter
  private final JmsHeaders headers;

  /**
   * The session ACK ID required to ACK a the current Message if one is available, or null otherwise.
   */
  @Parameter
  @Optional
  private final String ackId;

  /**
   * W3C traceparent of the receive span (set when tracing is enabled on the connection).
   * Pass it to the next publish as the "traceparent" user property to continue the trace.
   */
  @Parameter
  @Optional
  private final String traceparent;

  /**
   * W3C tracestate of the receive span.
   */
  @Parameter
  @Optional
  private final String tracestate;

  /**
   * Where the receive span's parent came from: "solace" (broker-updated Solace context),
   * "user-property" (traceparent message property) or "none" (new trace).
   */
  @Parameter
  @Optional
  private final String traceContextSource;

  public JmsAttributes(JmsMessageProperties properties, JmsHeaders headers, String ackId) {
    this(properties, headers, ackId, null, null, null);
  }

  public JmsAttributes(JmsMessageProperties properties, JmsHeaders headers, String ackId,
                       String traceparent, String tracestate, String traceContextSource) {
    this.properties = properties;
    this.headers = headers;
    this.ackId = ackId;
    this.traceparent = traceparent;
    this.tracestate = tracestate;
    this.traceContextSource = traceContextSource;
  }

  public JmsAttributes() {
    this.properties = null;
    this.headers = null;
    this.ackId = null;
    this.traceparent = null;
    this.tracestate = null;
    this.traceContextSource = null;
  }

  @Override
  public JmsMessageProperties getProperties() {
    return properties;
  }

  @Override
  public JmsHeaders getHeaders() {
    return headers;
  }

  @Override
  public String getAckId() {
    return ackId;
  }

  public String getTraceparent() {
    return traceparent;
  }

  public String getTracestate() {
    return tracestate;
  }

  public String getTraceContextSource() {
    return traceContextSource;
  }
}
