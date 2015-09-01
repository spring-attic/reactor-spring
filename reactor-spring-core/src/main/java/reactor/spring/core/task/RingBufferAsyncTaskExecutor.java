package reactor.spring.core.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanNameAware;
import reactor.Timers;
import reactor.core.processor.BaseProcessor;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.Assert;
import reactor.fn.timer.Timer;
import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;

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

	private ProducerType                      producerType;
	private WaitStrategy                      waitStrategy;
	private BaseProcessor<Runnable, Runnable> dispatcher;

	public RingBufferAsyncTaskExecutor() {
		this(Timers.globalOrNew());
	}

	public RingBufferAsyncTaskExecutor(Timer timer) {
		super(timer);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		if(producerType != null && producerType == ProducerType.SINGLE) {
			this.dispatcher = RingBufferProcessor.create(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : new BlockingWaitStrategy())
			);
		}else{
			this.dispatcher = RingBufferProcessor.share(
			  getName(),
			  getBacklog(),
			  (null != waitStrategy ? waitStrategy : new BlockingWaitStrategy())
			);
		}
		if(isAutoStartup()){
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
	 * Get the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType} this {@link reactor.jarjar.com.lmax
	 * .disruptor.RingBuffer} is using.
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
	 * @param producerType the {@link reactor.jarjar.com.lmax.disruptor.dsl.ProducerType}
	 */
	public void setProducerType(ProducerType producerType) {
		this.producerType = producerType;
	}

	/**
	 * Get the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy} this {@link reactor.jarjar.com.lmax.disruptor
	 * .RingBuffer} is using.
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
	 * @param waitStrategy the {@link reactor.jarjar.com.lmax.disruptor.WaitStrategy}
	 */
	public void setWaitStrategy(WaitStrategy waitStrategy) {
		this.waitStrategy = waitStrategy;
	}

	@Override
	protected BaseProcessor<Runnable, Runnable> getProcessor() {
		return dispatcher;
	}

}
