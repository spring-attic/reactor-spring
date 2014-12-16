package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import reactor.Environment;
import reactor.core.dispatch.AbstractLifecycleDispatcher;
import reactor.core.dispatch.WorkQueueDispatcher;
import reactor.fn.Consumer;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import reactor.timer.Timer;

/**
 * Implementation of an {@link org.springframework.core.task.AsyncTaskExecutor} that is backed by a Reactor {@link
 * reactor.event.dispatch.WorkQueueDispatcher}.
 *
 * @author Jon Brisbin
 * @since 1.1
 */
public class WorkQueueAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware {

	private final Logger log = LoggerFactory.getLogger(WorkQueueAsyncTaskExecutor.class);

	private ProducerType              producerType;
	private WaitStrategy              waitStrategy;
	private ApplicationEventPublisher eventPublisher;
	private WorkQueueDispatcher       workQueue;

	public WorkQueueAsyncTaskExecutor(Environment env) {
		this(env.getTimer());
	}

	public WorkQueueAsyncTaskExecutor(Timer timer) {
		super(timer);
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		this.workQueue = new WorkQueueDispatcher(
				getName(),
				getThreads(),
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
		return workQueue;
	}

}
