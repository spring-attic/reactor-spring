package reactor.spring.context;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.*;
import reactor.core.processor.RingBufferProcessor;
import reactor.core.support.NamedDaemonThreadFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link org.springframework.context.ApplicationEventPublisher} that uses a {@link
 * reactor.core.support.rb.disruptor.RingBuffer} to dispatch events.
 *
 * @author Jon Brisbin
 */
public class RingBufferApplicationEventPublisher implements ApplicationEventPublisher,
                                                            ApplicationContextAware,
                                                            SmartLifecycle {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final boolean                           autoStartup;
	private final RingBufferProcessor<ApplicationEvent> processor;

	private volatile boolean running = false;

	private ApplicationContext       appCtx;

	public RingBufferApplicationEventPublisher(int backlog, boolean autoStartup) {
		this.autoStartup = autoStartup;

		this.processor = RingBufferProcessor.share("ringBufferAppEventPublisher", backlog);

		if(autoStartup) {
			start();
		}
	}

	@Override
	public void setApplicationContext(ApplicationContext appCtx) throws BeansException {
		this.appCtx = appCtx;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		processor.onComplete();
		if(null != callback) {
			callback.run();
		}
		synchronized(this) {
			running = false;
		}
	}

	@Override
	public void start() {
		synchronized(this) {
			processor.subscribe(new Subscriber<ApplicationEvent>() {
				@Override
				public void onSubscribe(Subscription s) {
					s.request(Long.MAX_VALUE);
				}

				@Override
				public void onNext(ApplicationEvent applicationEvent) {
					appCtx.publishEvent(applicationEvent);
				}

				@Override
				public void onError(Throwable t) {
					log.error("", t);
				}

				@Override
				public void onComplete() {
					log.trace("AppEvent Publisher has shutdown");
				}
			});
			running = true;
		}
	}

	@Override
	public void stop() {
		stop(null);
	}

	@Override
	public boolean isRunning() {
		synchronized(this) {
			return running;
		}
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public void publishEvent(ApplicationEvent event) {
		processor.onNext(event);
	}

}
