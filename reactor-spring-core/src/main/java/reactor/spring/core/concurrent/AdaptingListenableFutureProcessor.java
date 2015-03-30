package reactor.spring.core.concurrent;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.util.concurrent.FailureCallback;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import org.springframework.util.concurrent.SuccessCallback;
import reactor.fn.Consumer;
import reactor.rx.Promise;
import reactor.rx.Promises;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Adapt a Reactor {@link reactor.rx.Promise} to a Spring {@link org.springframework.util.concurrent.ListenableFuture}
 * and possibly convert the value before setting it on the {@code Promise}.
 */
public abstract class AdaptingListenableFutureProcessor<T, V> implements ListenableFuture<V>, Processor<T, V> {

	private final AtomicBoolean cancelled = new AtomicBoolean();

	private final Promise<V> promise = Promises.prepare();

	@Override
	public void subscribe(Subscriber<? super V> s) {
		promise.subscribe(s);
	}

	@Override
	public void onSubscribe(Subscription s) {
		promise.onSubscribe(s);
	}

	@Override
	public void onNext(T t) {
		promise.onNext(adapt(t));
	}

	@Override
	public void onError(Throwable t) {
		promise.onError(t);
	}

	@Override
	public void onComplete() {
		promise.onComplete();
	}

	@Override
	public void addCallback(ListenableFutureCallback<? super V> callback) {
		addCallback(callback, callback);
	}

	@Override
	public void addCallback(final SuccessCallback<? super V> successCallback,
	                        final FailureCallback failureCallback) {
		promise
				.onSuccess(new Consumer<V>() {
					@Override
					public void accept(V val) {
						if (null != successCallback) {
							successCallback.onSuccess(val);
						}
					}
				})
				.onError(new Consumer<Throwable>() {
					@Override
					public void accept(Throwable throwable) {
						if (null != failureCallback) {
							failureCallback.onFailure(throwable);
						}
					}
				});
	}

	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		if (promise.isPending() && cancelled.compareAndSet(false, true)) {
			promise.onComplete();
			return true;
		} else {
			return cancelled.get();
		}
	}

	@Override
	public boolean isCancelled() {
		return cancelled.get();
	}

	@Override
	public boolean isDone() {
		return promise.isComplete();
	}

	@Override
	public V get() throws InterruptedException, ExecutionException {
		return promise.await();
	}

	@Override
	public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return promise.await(timeout, unit);
	}

	protected abstract V adapt(T val);

}
