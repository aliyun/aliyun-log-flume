package com.aliyun.loghub.flume.sink;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Sink;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.UUID;

import static com.aliyun.loghub.flume.LoghubConstants.SERIALIZER_KEY;
import static org.apache.flume.source.SpoolDirectorySourceConfigurationConstants.BATCH_SIZE;


public class TestLoghubSink {

    private Map<String, String> parameters;
    private LoghubSink fixture;

    private Channel bindAndStartChannel(LoghubSink fixture) {
        // Configure the channel
        Channel channel = new MemoryChannel();
        Configurables.configure(channel, new Context());
        // Wire them together
        fixture.setChannel(channel);
        fixture.start();
        return channel;
    }

    @Before
    public void init() throws Exception {
        fixture = new LoghubSink();
        fixture.setName("LoghubSink-" + UUID.randomUUID().toString());
    }

    @After
    public void tearDown() throws Exception {
        // No-op
    }

    @Test
    public void shouldIndexOneEvent() throws Exception {
        Configurables.configure(fixture, new Context(parameters));
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody("event #1 or 1".getBytes());
        channel.put(event);
        tx.commit();
        tx.close();

        fixture.process();
        fixture.stop();

        // check result
    }

    @Test
    public void shouldIndexInvalidComplexJsonBody() throws Exception {
        parameters.put(BATCH_SIZE, "3");
        Configurables.configure(fixture, new Context(parameters));
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event1 = EventBuilder.withBody("TEST1 {test}".getBytes());
        channel.put(event1);
        Event event2 = EventBuilder.withBody("{test: TEST2 }".getBytes());
        channel.put(event2);
        Event event3 = EventBuilder.withBody("{\"test\":{ TEST3 {test} }}".getBytes());
        channel.put(event3);
        tx.commit();
        tx.close();

        fixture.process();
        fixture.stop();
    }

    @Test
    public void shouldIndexComplexJsonEvent() throws Exception {
        Configurables.configure(fixture, new Context(parameters));
        Channel channel = bindAndStartChannel(fixture);

        Transaction tx = channel.getTransaction();
        tx.begin();
        Event event = EventBuilder.withBody(
                "{\"event\":\"json content\",\"num\":1}".getBytes());
        channel.put(event);
        tx.commit();
        tx.close();

        fixture.process();
        fixture.stop();
    }

    @Test
    public void shouldIndexFiveEvents() throws Exception {
        // Make it so we only need to call process once
        parameters.put(BATCH_SIZE, "5");
        Configurables.configure(fixture, new Context(parameters));
        Channel channel = bindAndStartChannel(fixture);

        int numberOfEvents = 5;
        Event[] events = new Event[numberOfEvents];

        Transaction tx = channel.getTransaction();
        tx.begin();
        for (int i = 0; i < numberOfEvents; i++) {
            String body = "event #" + i + " of " + numberOfEvents;
            Event event = EventBuilder.withBody(body.getBytes());
            events[i] = event;
            channel.put(event);
        }
        tx.commit();
        tx.close();

        fixture.process();
        fixture.stop();
    }

    @Test
    public void shouldIndexFiveEventsOverThreeBatches() throws Exception {
        parameters.put(BATCH_SIZE, "2");
        Configurables.configure(fixture, new Context(parameters));
        Channel channel = bindAndStartChannel(fixture);

        int numberOfEvents = 5;
        Event[] events = new Event[numberOfEvents];

        Transaction tx = channel.getTransaction();
        tx.begin();
        for (int i = 0; i < numberOfEvents; i++) {
            String body = "event #" + i + " of " + numberOfEvents;
            Event event = EventBuilder.withBody(body.getBytes());
            events[i] = event;
            channel.put(event);
        }
        tx.commit();
        tx.close();

        int count = 0;
        Sink.Status status = Sink.Status.READY;
        while (status != Sink.Status.BACKOFF) {
            count++;
            status = fixture.process();
        }
        fixture.stop();
    }

    @Test
    public void shouldUseSpecifiedSerializer() throws Exception {
        Context context = new Context();
        context.put(SERIALIZER_KEY, "json");
        fixture.configure(context);
    }
}