package com.reactor.debug;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import reactor.core.CoreSubscriber;
import reactor.core.Scannable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.function.BiFunction;

public final class ReactorTracer {

    private ReactorTracer() {

    }

    private static Tracer tracer;
    public static void disable() {
        Hooks.resetOnOperatorDebug();
        Hooks.resetOnEachOperator(TracingSubscriber.class.getName());
    }

    public static void enable(TracerProvider provider) {
        Hooks.onOperatorDebug();
        Hooks.onEachOperator(TracingSubscriber.class.getName(), Operators.lift(new Lifter<>()));
        tracer = provider.get("reactor-debug");
    }

    static class Lifter<T>
            implements BiFunction<Scannable, CoreSubscriber<? super T>, CoreSubscriber<? super T>> {

        private static final String CONTEXT_KEY = "otel-context";
        @Override
        public CoreSubscriber<? super T> apply(Scannable publisher, CoreSubscriber<? super T> sub) {
            Context parent = sub.currentContext().getOrDefault(CONTEXT_KEY, Context.current());

            Scannable source = publisher.scan(Scannable.Attr.PARENT);
            String name = source == null ? publisher.toString() : publisher + " : " + source;

            Span span = tracer.spanBuilder(name).setParent(parent).startSpan();
            if (publisher instanceof Mono) {
                ((Mono<?>) publisher).contextWrite(reactor.util.context.Context.of(CONTEXT_KEY, parent.with(span)));
            } else if (publisher instanceof Flux) {
                ((Flux<?>) publisher).contextWrite(reactor.util.context.Context.of(CONTEXT_KEY, parent.with(span)));
            }

            try (Scope ignored = span.makeCurrent()) {
                return new TracingSubscriber<>(publisher, sub, span);
            }
        }
    }

    // heavily based on https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/instrumentation/reactor-3.1/library/src/main/java/io/opentelemetry/instrumentation/reactor/TracingSubscriber.java
    static class TracingSubscriber<T> implements CoreSubscriber<T> {

        Subscriber sub;
        private final Span span;
        private final reactor.util.context.Context context;
        public TracingSubscriber(Scannable publisher, CoreSubscriber<? super T> subscriber, Span span) {
            this.sub = subscriber;
            this.context = subscriber.currentContext();
            this.span = span;
        }

        @Override
        public void onSubscribe(Subscription s) {
            span.addEvent("onSubscribe");
            try (Scope ss = span.makeCurrent()) {
                this.sub.onSubscribe(s);
            }
        }

        @Override
        public void onNext(T t) {
            span.addEvent("onNext");
            try (Scope s = span.makeCurrent()) {
                this.sub.onNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            span.recordException(t);
            try (Scope s = span.makeCurrent()) {
                this.sub.onError(t);
            }
        }

        @Override
        public reactor.util.context.Context currentContext() {
            return context;
        }

        @Override
        public void onComplete() {
            try (Scope ignored = span.makeCurrent()) {
                this.sub.onComplete();
            }
            span.end();
        }
    }
}
