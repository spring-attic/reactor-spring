package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.WorkQueueProcessor;
import reactor.core.scheduler.Schedulers;
import reactor.core.scheduler.TimedScheduler;
import reactor.util.concurrent.WaitStrategy;

import org.springframework.context.ApplicationEventPublisherAware;

/**
 * Implementation of an {@link org.springframework.core.task.AsyncTaskExecutor} that is backed by a Reactor {@link
 * WorkQueueProcessor}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.1, 2.5
 */
public class WorkQueueAsyncTaskExecutor extends AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware {

	private final Logger log = LoggerFactory.getLogger(WorkQueueAsyncTaskExecutor.class);

	private WaitStrategy                      waitStrategy;
	private WorkQueueProcessor<Runnable> workQueue;

	public WorkQueueAsyncTaskExecutor() {
		this(Schedulers.timer());
	}

	public WorkQueueAsyncTaskExecutor(TimedScheduler timer) {
		super(timer);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if (!isShared()) {
			this.workQueue = WorkQueueProcessor.create(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : WaitStrategy.blocking())
			);
		} else {
			this.workQueue = WorkQueueProcessor.share(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : WaitStrategy.blocking())
			);
		}
		if (isAutoStartup()) {
			start();
		}
	}

	/**
	 * Get the {@link reactor.util.concurrent.WaitStrategy} this {@link reactor.util.concurrent.RingBuffer} is using.
	 *
	 * @return the {@link reactor.util.concurrent.WaitStrategy}
	 */
	public WaitStrategy getWaitStrategy() {
		return waitStrategy;
	}

	/**
	 * Set the {@link reactor.util.concurrent.WaitStrategy} to use when creating the internal {@link
	 * reactor.util.concurrent.RingBuffer}.
	 *
	 * @param waitStrategy
	 * 		the {@link reactor.util.concurrent.WaitStrategy}
	 */
	public void setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
	}

	@Override
	protected WorkQueueProcessor<Runnable> getProcessor() {
		return workQueue;
	}

}
