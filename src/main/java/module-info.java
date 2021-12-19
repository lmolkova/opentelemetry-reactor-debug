module opentelemetry.reactor.debug {
    requires reactor.core;
    requires io.opentelemetry.api;
    requires io.opentelemetry.context;
    requires org.reactivestreams;

    exports com.reactor.debug;
}