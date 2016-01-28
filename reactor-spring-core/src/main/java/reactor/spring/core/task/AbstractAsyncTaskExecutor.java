/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.spring.core.task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.ExecutorProcessor;
import reactor.core.state.Pausable;
import reactor.core.timer.Timer;
import reactor.core.util.Exceptions;
import reactor.fn.Consumer;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.SmartLifecycle;
import org.springframework.core.task.AsyncListenableTaskExecutor;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureTask;

/**
 * Abstract base class for {@link org.springframework.core.task.AsyncTaskExecutor} implementations that need some basic
 * metadata about how they should be configured.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @since 1.1, 2.5
 */
public abstract class AbstractAsyncTaskExecutor implements ApplicationEventPublisherAware,
  ScheduledExecutorService,
  AsyncListenableTaskExecutor,
  InitializingBean,
  SmartLifecycle,
  Subscriber<Runnable> {

	private final Logger log = LoggerFactory.getLogger(getClass());
	private final Timer timer;

	private final AtomicBoolean running = new AtomicBoolean(false);

	private String name    = getClass().getSimpleName();
	private int    threads = Runtime.getRuntime().availableProcessors();
	private int    backlog = 2048;
	private boolean shared = true;

	private ApplicationEventPublisher eventPublisher;

	protected AbstractAsyncTaskExecutor(Timer timer) {
		this.timer = timer;
	}


	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher eventPublisher) {
		this.eventPublisher = eventPublisher;
	}

	@Override
	public boolean isAutoStartup() {
		return true;
	}

	@Override
	public void stop(Runnable callback) {
		if (running.compareAndSet(true, false)) {
			getProcessor().awaitAndShutdown();
			callback.run();
		}
	}

	@Override
	public void start() {
		if (running.compareAndSet(false, true)) {
			for (int i = 0; i < getThreads(); i++) {
				getProcessor().subscribe(this);
			}
			getProcessor().start();
		}
	}

	@Override
	public void stop() {
		if (running.compareAndSet(true, false)){
			getProcessor().awaitAndShutdown();
		}
	}

	@Override
	public boolean isRunning() {
		return getProcessor().alive() && running.get();
	}

	@Override
	public int getPhase() {
		return 0;
	}

	@Override
	public void onSubscribe(Subscription s) {
		s.request(Long.MAX_VALUE);
	}

	@Override
	public void onNext(Runnable runnable) {
		try {
			runnable.run();
		} catch (Throwable t) {
			Exceptions.throwIfFatal(t);
			onError(t);
		}
	}

	@Override
	public void onError(Throwable t) {
		if (null != eventPublisher) {
			eventPublisher.publishEvent(new AsyncTaskExceptionEvent(t));
		} else {
			log.error(t.getMessage(), t);
		}
	}

	@Override
	public void onComplete() {
		timer.cancel();
		log.trace(getName() + " task executor has shutdown");
	}

	/**
	 * Get the name by which these threads will be known.
	 *
	 * @return name of the threads for this work queue
	 */
	public String getName() {
		return name;
	}

	/**
	 * Set the name by which these threads are known.
	 *
	 * @param name name of the threads for this work queue
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * Get the number of threads being used by this executor.
	 *
	 * @return the number of threads being used
	 */
	public int getThreads() {
		return threads;
	}

	/**
	 * Set the number of threads to use when creating this executor.
	 *
	 * @param threads the number of threads to use
	 */
	public void setThreads(int threads) {
		this.threads = threads;
	}

	/** Can this executor be called from multiple threads ?
	 *
	 * @return true if multithread input ready
	 */
	public boolean isShared() {
		return shared;
	}


	/**
	 * Tells this executor if it will be accepting tasks from multiple threads
	 *
	 * @param shared True if should support multithread publishing
	 */
	public void setShared(boolean shared) {
		this.shared = shared;
	}

	/**
	 * Get the number of pre-allocated tasks to keep in memory. Correlates directly to the size of the internal {@code
	 * RingBuffer}.
	 *
	 * @return the backlog value
	 */
	public int getBacklog() {
		return backlog;
	}

	/**
	 * Set the number of pre-allocated tasks to keep in memory. Correlates directly to the size of the internal {@code
	 * RingBuffer}.
	 *
	 * @param backlog the backlog value
	 */
	public void setBacklog(int backlog) {
		this.backlog = backlog;
	}


	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		return getProcessor().awaitAndShutdown(timeout, unit);
	}

	@Override
	public boolean isTerminated() {
		return !getProcessor().alive();
	}

	@Override
	public boolean isShutdown() {
		return isTerminated();
	}

	@Override
	public List<Runnable> shutdownNow() {
		shutdown();
		return Collections.emptyList();
	}

	@Override
	public void shutdown() {
		if(running.compareAndSet(true, false)) {
			getProcessor().shutdown();
		}
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
	                       long timeout,
	                       TimeUnit unit) throws InterruptedException,
	  ExecutionException,
	  TimeoutException {
		List<FutureTask<T>> submittedTasks = new ArrayList<FutureTask<T>>();
		for (Callable<T> task : tasks) {
			FutureTask<T> ft = new FutureTask<T>(task);
			execute(ft);
			submittedTasks.add(ft);
		}

		T result = null;
		long start = System.currentTimeMillis();
		for (; ; ) {
			for (FutureTask<T> task : submittedTasks) {
				result = task.get(100, TimeUnit.MILLISECONDS);
				if (null != result || task.isDone()) {
					break;
				}
			}
			if (null != result || (System.currentTimeMillis() - start) > TimeUnit.MILLISECONDS.convert(timeout,
			  unit)) {
				break;
			}
		}
		for (FutureTask<T> task : submittedTasks) {
			if (!task.isDone()) {
				task.cancel(true);
			}
		}
		return result;
	}

	@Override
	public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException,
	  ExecutionException {
		try {
			return invokeAny(tasks, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
		} catch (TimeoutException e) {
			throw new ExecutionException(e.getMessage(), e);
		}
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
	                                     long timeout,
	                                     TimeUnit unit) throws InterruptedException {
		List<Future<T>> submittedTasks = new ArrayList<Future<T>>();
		for (Callable<T> task : tasks) {
			FutureTask<T> ft = new FutureTask<T>(task);
			execute(ft);
			submittedTasks.add(ft);
		}

		T result = null;
		long start = System.currentTimeMillis();
		for (; ; ) {
			boolean allComplete = false;
			for (Future<T> task : submittedTasks) {
				try {
					result = task.get(100, TimeUnit.MILLISECONDS);
				} catch (ExecutionException e) {
					log.error(e.getMessage(), e);
				} catch (TimeoutException e) {
					log.error(e.getMessage(), e);
				}
				if (allComplete = !allComplete && task.isDone()) {
					break;
				}
			}
			if (null != result || (System.currentTimeMillis() - start) > TimeUnit.MILLISECONDS.convert(timeout,
			  unit)) {
				break;
			}
		}
		return submittedTasks;
	}

	@Override
	public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
		return invokeAll(tasks, Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
	}

	@Override
	public void execute(final Runnable task, long startTimeout) {
		timer.submit(
		  new Consumer<Long>() {
			  @Override
			  public void accept(Long now) {
				  execute(task);
			  }
		  },
		  startTimeout,
		  TimeUnit.MILLISECONDS
		);
	}

	@Override
	public Future<?> submit(Runnable task) {
		final FutureTask<Void> future = new FutureTask<Void>(task, null);
		execute(future);
		return future;
	}

	@Override
	public <T> Future<T> submit(Callable<T> task) {
		final FutureTask<T> future = new FutureTask<T>(task);
		execute(future);
		return future;
	}

	@Override
	public <T> Future<T> submit(Runnable task, T result) {
		FutureTask<T> future = new FutureTask<T>(task, result);
		execute(future);
		return future;
	}

	@Override
	public ListenableFuture<?> submitListenable(Runnable task) {
		ListenableFutureTask<?> f = new ListenableFutureTask<Object>(task, null);
		submit(f);
		return f;
	}

	@Override
	public <T> ListenableFuture<T> submitListenable(Callable<T> task) {
		ListenableFutureTask<T> f = new ListenableFutureTask<T>(task);
		submit(f);
		return f;
	}

	@Override
	public void execute(Runnable task) {
		getProcessor().onNext(task);
	}

	@Override
	public ScheduledFuture<?> schedule(Runnable command,
	                                   long delay,
	                                   TimeUnit unit) {
		long initialDelay = convertToMillis(delay, unit);
		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(command, null, initialDelay);
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(future);
			}
		}, initialDelay, TimeUnit.MILLISECONDS);
		return future;
	}

	@Override
	public <V> ScheduledFuture<V> schedule(Callable<V> callable,
	                                       long delay,
	                                       TimeUnit unit) {
		long initialDelay = convertToMillis(delay, unit);
		final ScheduledFutureTask<V> future = new ScheduledFutureTask<V>(callable, initialDelay);
		timer.submit(new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(future);
			}
		}, initialDelay, TimeUnit.MILLISECONDS);
		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleAtFixedRate(final Runnable command,
	                                              long initialDelay,
	                                              long period,
	                                              TimeUnit unit) {
		long initialDelayInMs = convertToMillis(initialDelay, unit);
		long periodInMs = convertToMillis(period, unit);
		final AtomicReference<Pausable> registration = new AtomicReference<Pausable>();

		final Runnable task = new Runnable() {
			@Override
			public void run() {
				try {
					command.run();
				} catch (Throwable t) {
					log.error(t.getMessage(), t);
					Pausable reg;
					if (null != (reg = registration.get())) {
						reg.cancel();
					}
				}
			}
		};

		final Consumer<Long> consumer = new Consumer<Long>() {
			@Override
			public void accept(Long now) {
				execute(task);
			}
		};

		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(task, null, initialDelay);
		registration.set(timer.schedule(consumer, periodInMs, TimeUnit.MILLISECONDS, initialDelayInMs));
		return future;
	}

	@Override
	public ScheduledFuture<?> scheduleWithFixedDelay(final Runnable command,
	                                                 long initialDelay,
	                                                 long delay,
	                                                 TimeUnit unit) {
		final long initialDelayInMs = convertToMillis(initialDelay, unit);
		final long delayInMs = convertToMillis(initialDelay, unit);
		final ScheduledFutureTask<?> future = new ScheduledFutureTask<Object>(command, null, initialDelayInMs);

		final AtomicReference<Pausable> registration = new AtomicReference<Pausable>();

		final Consumer<Long> consumer = new Consumer<Long>() {
			final Consumer<Long> self = this;

			@Override
			public void accept(Long now) {
				execute(new Runnable() {
					@Override
					public void run() {
						try {
							future.run();
							timer.submit(self, delayInMs, TimeUnit.MILLISECONDS);
						} catch (Throwable t) {
							log.error(t.getMessage(), t);
							Pausable reg;
							if (null != (reg = registration.get())) {
								reg.cancel();
							}
						}
					}
				});
			}
		};

		registration.set(timer.submit(consumer, initialDelayInMs, TimeUnit.MILLISECONDS));
		return future;
	}

	protected abstract ExecutorProcessor<Runnable, Runnable> getProcessor();

	private static long convertToMillis(long l, TimeUnit timeUnit) {
		if (timeUnit == TimeUnit.MILLISECONDS) {
			return l;
		} else {
			return timeUnit.convert(l, TimeUnit.MILLISECONDS);
		}
	}

	private static class ScheduledFutureTask<T> extends FutureTask<T> implements ScheduledFuture<T> {
		private final long delay;

		private ScheduledFutureTask(Runnable runnable, T result, long delay) {
			super(runnable, result);
			this.delay = delay;
		}

		private ScheduledFutureTask(Callable<T> callable, long delay) {
			super(callable);
			this.delay = delay;
		}

		@Override
		public long getDelay(TimeUnit unit) {
			return convertToMillis(delay, unit);
		}

		@Override
		public int compareTo(Delayed d) {
			if (this == d) {
				return 0;
			}
			long diff = getDelay(TimeUnit.MILLISECONDS) - d.getDelay(TimeUnit.MILLISECONDS);
			return (diff == 0 ? 0 : ((diff < 0) ? -1 : 1));
		}
	}
}
