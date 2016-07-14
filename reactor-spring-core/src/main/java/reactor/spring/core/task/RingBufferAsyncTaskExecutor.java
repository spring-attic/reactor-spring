package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.TopicProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.WaitStrategy;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.util.Assert;

/**
 * Implementation of {@link org.springframework.core.task.AsyncTaskExecutor} that uses a {@link TopicProcessor}
 * to
 * execute tasks.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.1, 2.5
 */
public class RingBufferAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements BeanNameAware {

	private final Logger log = LoggerFactory.getLogger(RingBufferAsyncTaskExecutor.class);

	private WaitStrategy                          waitStrategy;
	private TopicProcessor<Runnable> dispatcher;

	public RingBufferAsyncTaskExecutor() {
		this(Schedulers.timer());
	}

	public RingBufferAsyncTaskExecutor(TimedScheduler timer) {
		super(timer);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!isShared()) {
			this.dispatcher = TopicProcessor.create(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : WaitStrategy.blocking())
			);
		} else {
			this.dispatcher = TopicProcessor.share(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : WaitStrategy.blocking())
			);
		}
		if (isAutoStartup()) {
			start();
		}
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
	 * Get the {@link reactor.util.WaitStrategy} this {@link reactor.core.queue
	 * .RingBuffer} is using.
	 *
	 * @return the {@link reactor.util.WaitStrategy}
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Set the {@link reactor.util.WaitStrategy} to use when creating the internal {@link
	 * reactor.core.queue.RingBuffer}.
	 *
	 * @param waitStrategy the {@link reactor.util.WaitStrategy}
	 */
	public void setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
	}

	@Override
	protected TopicProcessor<Runnable> getProcessor() {
		return dispatcher;
	}

}
