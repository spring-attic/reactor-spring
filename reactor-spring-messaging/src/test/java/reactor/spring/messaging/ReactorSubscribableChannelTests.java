package reactor.spring.messaging;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.spring.context.config.EnableReactor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * @author Jon Brisbin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactorSubscribableChannelTests {

	static final Logger LOG  = LoggerFactory.getLogger(ReactorSubscribableChannelTests.class);
	static final int    MSGS = 50000000;

	@Autowired
	Reactor reactor;

	CountDownLatch latch;
	long           start;
	long           end;
	double         elapsed;
	int            throughput;

	@Before
	public void init() {
		latch = new CountDownLatch(MSGS);
	}

	@After
	public void cleanup() {
		end = System.currentTimeMillis();
		elapsed = end - start;
		throughput = (int) (MSGS / (elapsed / 1000));
		LOG.info("Processed {} msgs in {}ms for throughput of {}/sec", MSGS, (long) elapsed, throughput);
	}

	@Test
	public void reactorProcessorChannelThroughputTest() throws InterruptedException {
		ReactorSubscribableChannel channel = new ReactorSubscribableChannel();

		channel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
			}
		});

		Message<?> msg = MessageBuilder.withPayload("Hello World!").build();
		start = System.currentTimeMillis();
		for (int i = 0; i < MSGS; i++) {
			channel.send(msg);
		}

		assertTrue("latch did not time out", latch.await(1, TimeUnit.SECONDS));
	}

	@Test
	public void reactorChannelThroughputTest() throws InterruptedException {
		org.springframework.messaging.support.channel.ReactorSubscribableChannel channel =
				new org.springframework.messaging.support.channel.ReactorSubscribableChannel(reactor);

		channel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
			}
		});

		Message<?> msg = MessageBuilder.withPayload("Hello World!").build();
		start = System.currentTimeMillis();
		for (int i = 0; i < MSGS; i++) {
			channel.send(msg);
		}

		assertTrue("latch did not time out", latch.await(1, TimeUnit.SECONDS));
	}

	@Configuration
	@EnableReactor
	static class ReactorConfig {

		@Bean
		public Reactor reactor(Environment env) {
			return Reactors.reactor().env(env).dispatcher(Environment.RING_BUFFER).get();
		}

	}

}
