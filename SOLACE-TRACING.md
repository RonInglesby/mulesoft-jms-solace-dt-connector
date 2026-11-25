# Solace tracing for the MuleSoft JMS connector

This branch adds OpenTelemetry tracing to the standard MuleSoft JMS connector, including Solace's native (broker-level) trace context. Your flows keep the standard `jms:` components (listener, publish, consume, publish-consume, ack, recover-session), and transactions keep working as they do today.

Apps using Solace through the **Generic Connection** get one trace across Mule and the broker:

```
mule-orders  orders/created send         (jms:publish)
  └ broker spans                          (Solace distributed tracing)
     └ mule-orders  q1 receive            (jms:listener / consume)
        └ mule-orders  order/processed send
           └ broker spans
              └ mule-orders  q2 receive ...
```

## How it works

Tracing is enabled on the Generic Connection. The connector then wraps the JMS connection factory it was given. Every connection, session, producer and consumer created from it is traced, whichever internal class does the sending or receiving.

| When | Span | Parent |
|---|---|---|
| Any send or publish (publish, publish-consume request, listener reply) | PRODUCER `<destination> send` | The `traceparent` user property the flow set on the message, otherwise a new trace |
| Any receive (listener, consume, publish-consume reply) | CONSUMER `<destination> receive` | The Solace-native context, which the broker has replaced with its own span, otherwise the `traceparent` user property |

**On send.** The span's context is written onto the message in two ways:

- as `traceparent`/`tracestate` user properties;
- for Solace JMS messages, into the Solace-native context the broker reads (`SolaceJmsW3CTextMapSetter`).

**On receive.** The receive span's context is exposed to the flow as `attributes.traceparent` and `attributes.tracestate`. `attributes.traceContextSource` says where its parent came from:

- `solace` — the broker-updated context; this is what you want;
- `user-property` — the broker didn't trace the message;
- `none` — the receive started a new trace.

**Continuing the trace in a flow.** Put the receive span's context into the next publish's user properties:

```xml
<jms:publish config-ref="SOLACE_JMS_CONFIG" destination="order/processed" destinationType="TOPIC">
    <jms:message>
        <jms:properties>#[if (attributes.traceparent != null) {traceparent: attributes.traceparent} else {}]</jms:properties>
    </jms:message>
</jms:publish>
```

**When tracing is disabled** (the default), nothing is wrapped and the connector behaves exactly like the original.

## Configuration

Settings are on the new **Tracing** tab of the Generic Connection in Studio:

```xml
<jms:config name="SOLACE_JMS_CONFIG">
    <jms:generic-connection username="default" password="default"
        tracingEnabled="true"
        tracingServiceName="mule-orders"
        tracingOtlpEndpoint="${otel.endpoint}">
        <jms:connection-factory>
            <jms:jndi-connection-factory connectionFactoryJndiName="/jms/cf/default" lookupDestination="TRY_ALWAYS">
                <!-- unchanged -->
            </jms:jndi-connection-factory>
        </jms:connection-factory>
    </jms:generic-connection>
</jms:config>
```

| Attribute | Default | Notes |
|---|---|---|
| `tracingEnabled` | `false` | Turns tracing on for this connection |
| `tracingServiceName` | `mule-jms` | `service.name` in Jaeger. Use separate `jms:config`s if you want separate services per flow |
| `tracingOtlpEndpoint` | `http://localhost:4318/v1/traces` | OTLP/HTTP endpoint of your collector |

## Build and use

1. **Build the connector:**

Install:

Eclipse Adoptium jdk-11.0.33.10-hotspot

https://adoptium.net/en-GB/temurin/releases?version=11

Use Settings:
   ```bash
set JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-11.0.33.10-hotspot
set PATH=%JAVA_HOME%\bin;%PATH%
set MAVEN_OPTS=
   ```

Build:
   ```bash
   mvn clean install -DskipTests -Dmdep.skip=true
   ```

   This installs `org.mule.connectors:mule-jms-connector:1.11.0-SOLACE-TRACING-3` locally. The build needs access to the MuleSoft Maven repositories, as the original connector does. Tests are skipped because the connector's test suite needs Docker and an ActiveMQ broker.

2. **Point the app's JMS connector dependency at that version:**

   ```xml
   <dependency>
       <groupId>org.mule.connectors</groupId>
       <artifactId>mule-jms-connector</artifactId>
       <version>1.11.0-SOLACE-TRACING-3</version>
       <classifier>mule-plugin</classifier>
   </dependency>
   ```

3. **Keep the Solace JMS API (`sol-jms`) as a shared library of the app**, as for any JMS vendor client. The tracing code reads Solace messages through it. It isn't bundled in the connector.

4. **Remove the old tracing code from the app.** That means the Java Module calls, `TraceHelper`, `SolacePublisher`, and the OpenTelemetry and Solace OpenTelemetry dependencies.

## Notes and limits

- **XA isn't traced.** Messages on XA connection factories are sent and received exactly as before, but without spans. Local (non-XA) transactions are traced normally.
- **Receive spans cover receipt only.** The flow continues the trace by passing `attributes.traceparent` on, as above. Listener replies follow the same rule: set the `traceparent` property in `<jms:response>`.
- **Unsupported message IDs.** If a producer disables message IDs, the receive span can't be linked to the attributes. The flow then gets the sender's `traceparent` property instead.
- **Solace-native context not readable.** If the app doesn't share `sol-jms` with the connector, or the integration classes can't load, the connector logs one warning and falls back to the `traceparent` user properties. Messaging is never interrupted by a tracing failure.
- **The startup log line** `JMS tracing enabled ... Solace-native context available` confirms the setup.

## Changed files

- `pom.xml` — the version and the OpenTelemetry and Solace OpenTelemetry JMS integration dependencies.
- `internal/tracing/*` — new: settings, tracer, JMS proxies, the Solace-native bridge and the receive registry.
- `internal/connection/provider/GenericConnectionProvider.java` — the Tracing tab, factory wrapping, and tracer shutdown.
- `api/message/JmsAttributes.java` — `traceparent`, `tracestate` and `traceContextSource`.
- `internal/message/JmsConnectorAttributeBuilder.java` — fills those attributes.
