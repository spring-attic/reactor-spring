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
import reactor.Environment;
import reactor.event.EventBus;
import reactor.spring.context.config.EnableReactor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Comparing a Reactor {@code Processor}-backed Channel to a plain {@code Reactor}-backed Channel for comparing the net
 * effect of dynamic event routing on total throughput. It's pretty clear here that a simple Channel doesn't benefit
 * from the Reactor dynamic event dispatching and is better-suited to the single-task {@code Processor}.
 *
 * @author Jon Brisbin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class ReactorSubscribableChannelThroughputTests {

	static final Logger LOG  = LoggerFactory.getLogger(ReactorSubscribableChannelThroughputTests.class);
	static final int    MSGS = 5000;

	@Autowired
	EventBus eventBus;

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
		throughput = (int)(MSGS / (elapsed / 1000));
		LOG.info("Processed {} msgs in {}ms for throughput of {}/sec", MSGS, (long)elapsed, throughput);
	}

	@Test
	public void reactorSubscribableChannelMultiProducerThroughput() throws InterruptedException {
		doTest(new ReactorSubscribableChannel());
	}

	@Test
	public void reactorSubscribableChannelSingleProducerThroughput() throws InterruptedException {
		doTest(new ReactorSubscribableChannel(true));
	}

	private void doTest(ReactorSubscribableChannel channel) throws InterruptedException {
		channel.subscribe(new MessageHandler() {
			@Override
			public void handleMessage(Message<?> message) throws MessagingException {
				latch.countDown();
			}
		});

		Message<?> msg = MessageBuilder.withPayload("Hello World!").build();
		start = System.currentTimeMillis();
		for(int i = 0; i < MSGS; i++) {
			channel.send(msg);
		}
		assertTrue("latch did not time out", latch.await(5, TimeUnit.SECONDS));
	}

	@Configuration
	@EnableReactor
	static class ReactorConfig {

		@Bean
		public EventBus eventBus(Environment env) {
			return EventBus.create(env, Environment.RING_BUFFER);
		}

	}

}
