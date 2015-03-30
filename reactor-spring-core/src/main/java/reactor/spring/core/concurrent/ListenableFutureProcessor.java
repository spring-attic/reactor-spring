package reactor.spring.core.concurrent;

/**
 * Adapt a Reactor {@link reactor.rx.Promise} to a Spring {@link org.springframework.util.concurrent.ListenableFuture}.
 */
public class ListenableFutureProcessor<T> extends AdaptingListenableFutureProcessor<T, T> {

	@Override
	protected T adapt(T val) {
		return val;
	}

}
