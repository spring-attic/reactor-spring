package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Timers;
import reactor.core.processor.ExecutorProcessor;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.core.support.WaitStrategy;
import reactor.core.timer.Timer;

import org.springframework.beans.factory.BeanNameAware;

/**
 * Implementation of {@link org.springframework.core.task.AsyncTaskExecutor} that uses a {@link RingBufferProcessor}
 * to
 * execute tasks.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.1, 2.1
 */
public class RingBufferAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements BeanNameAware {

	private final Logger log = LoggerFactory.getLogger(RingBufferAsyncTaskExecutor.class);

	private WaitStrategy                          waitStrategy;
	private ExecutorProcessor<Runnable, Runnable> dispatcher;

	public RingBufferAsyncTaskExecutor() {
		this(Timers.globalOrNew());
	}

	public RingBufferAsyncTaskExecutor(Timer timer) {
		super(timer);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!isShared()) {
			this.dispatcher = RingBufferProcessor.create(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : new WaitStrategy.Blocking())
			);
		} else {
			this.dispatcher = RingBufferProcessor.share(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : new WaitStrategy.Blocking())
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
	 * Get the {@link reactor.core.support.WaitStrategy} this {@link reactor.core.support.rb.disruptor
	 * .RingBuffer} is using.
	 *
	 * @return the {@link reactor.core.support.WaitStrategy}
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Set the {@link reactor.core.support.WaitStrategy} to use when creating the internal {@link
	 * reactor.core.support.rb.disruptor.RingBuffer}.
	 *
	 * @param waitStrategy the {@link reactor.core.support.WaitStrategy}
	 */
	public void setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
	}

	@Override
	protected ExecutorProcessor<Runnable, Runnable> getProcessor() {
		return dispatcher;
	}

}
