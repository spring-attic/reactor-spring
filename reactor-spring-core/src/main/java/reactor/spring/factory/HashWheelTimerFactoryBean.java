package reactor.spring.factory;

import org.springframework.beans.factory.FactoryBean;
import reactor.fn.Supplier;
import reactor.fn.timer.HashWheelTimer;
import reactor.fn.timer.Timer;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link org.springframework.beans.factory.FactoryBean} implementation that provides {@link
 * reactor.fn.timer.HashWheelTimer} in a round-robin fashion. The default is to create a single timer but
 * using the {@link #HashWheelTimerFactoryBean(int, int)} constructor, one can create a "pool" of timers which will be
 * handed out to requestors in a round-robin fashion.
 *
 * @author Jon Brisbin
 */
public class HashWheelTimerFactoryBean implements FactoryBean<Timer> {

	private final Supplier<Timer> timers;

	/**
	 * Create a single {@link reactor.fn.timer.HashWheelTimer} with a default resolution of 50 milliseconds.
	 */
	public HashWheelTimerFactoryBean() {
		this(1, 50);
	}

	/**
	 * Create {@code numOfTimers} number of {@link reactor.fn.timer.HashWheelTimer HashWheelTimers}.
	 *
	 * @param numOfTimers
	 * 		the number of timers to create
	 * @param resolution
	 * 		the resolution of the timers, in milliseconds
	 */
	public HashWheelTimerFactoryBean(int numOfTimers, int resolution) {
		Timer[] timers = new Timer[numOfTimers];
		for(int i = 0; i < numOfTimers; i++) {
			timers[i] = new HashWheelTimer(resolution);
		}

		final AtomicInteger count = new AtomicInteger();
		final int len = timers.length;

		this.timers = new Supplier<Timer>() {
			@Override public Timer get() {
				return timers[count.getAndIncrement() % len];
			}
		};
	}

	@Override
	public Timer getObject() throws Exception {
		return timers.get();
	}

	@Override public Class<?> getObjectType() {
		return HashWheelTimer.class;
	}

	@Override public boolean isSingleton() {
		return false;
	}

}
