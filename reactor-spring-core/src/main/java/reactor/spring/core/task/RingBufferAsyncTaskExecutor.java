package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import reactor.Environment;
import reactor.core.dispatch.AbstractLifecycleDispatcher;
import reactor.core.dispatch.RingBufferDispatcher;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.timer.Timer;

/**
 * Implementation of {@link org.springframework.core.task.AsyncTaskExecutor} that uses a {@link RingBufferDispatcher}
 * to
 * execute tasks.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class RingBufferAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware,
                                                                                      BeanNameAware {

	private final Logger log = LoggerFactory.getLogger(RingBufferAsyncTaskExecutor.class);

	private ProducerType              producerType;
	private WaitStrategy              waitStrategy;
	private ApplicationEventPublisher eventPublisher;
	private RingBufferDispatcher      dispatcher;

	public RingBufferAsyncTaskExecutor(Environment env) {
		this(env.getTimer());
	}

	public RingBufferAsyncTaskExecutor(Timer timer) {
		super(timer);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.dispatcher = new RingBufferDispatcher(
				getName(),
				getBacklog(),
				new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						if (null != eventPublisher) {
							eventPublisher.publishEvent(new AsyncTaskExceptionEvent(throwable));
						} else {
							log.error(throwable.getMessage(), throwable);
						}
					}
				},
				(null != producerType ? producerType : ProducerType.MULTI),
				(null != waitStrategy ? waitStrategy : new BlockingWaitStrategy())
		);
	}

	@Override
	public void setBeanName(String name) {
		setName(name);
	}

	@Override
	public int getThreads() {
		// RingBufferDispatchers are always single-threaded
		return 1;
	}

	@Override
	public void setThreads(int threads) {
		Assert.isTrue(threads == 1, "A RingBufferAsyncTaskExecutor is always single-threaded");
		log.warn("RingBufferAsyncTaskExecutors are always single-threaded. Ignoring request to use " +
				         threads +
				         " threads.");
	}

	/**
	 * Get the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType} this {@link reactor.jarjar.com.lmax.disruptor.RingBuffer} is using.
	 *
	 * @return the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType}
	 */
	public ProducerType getProducerType() {
		return producerType;
	}

	/**
	 * Set the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType} to use when creating the internal {@link
	 * reactor.jarjar.com.lmax.disruptor.RingBuffer}.
	 *
	 * @param producerType
	 * 		the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType}
	 */
	public void setProducerType(ProducerType producerType) {
		this.producerType = producerType;
	}

	/**
	 * Get the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy} this {@link reactor.jarjar.com.lmax.disruptor.RingBuffer} is using.
	 *
	 * @return the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy}
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Set the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy} to use when creating the internal {@link
	 * reactor.jarjar.com.lmax.disruptor.RingBuffer}.
	 *
	 * @param waitStrategy
	 * 		the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy}
	 */
	public void setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
	}

	@Override
	protected AbstractLifecycleDispatcher getDispatcher() {
		return dispatcher;
	}

}
