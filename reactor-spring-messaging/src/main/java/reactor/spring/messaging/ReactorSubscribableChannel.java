package reactor.spring.messaging;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.ObjectUtils;
import reactor.bus.ringbuffer.Operation;
import reactor.bus.ringbuffer.RingBatcher;
import reactor.bus.ringbuffer.spec.RingBatcherSpec;
import reactor.fn.Consumer;
import reactor.fn.Supplier;
import reactor.fn.support.DelegatingConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscribable {@link org.springframework.messaging.MessageChannel} implementation that uses the RinBuffer-based
 * Reactor {@link reactor.bus.ringbuffer.RingBatcher} to publish messages for efficiency at high volumes.
 *
 * @author Jon Brisbin
 */
public class ReactorSubscribableChannel implements BeanNameAware, MessageChannel, SubscribableChannel {

	private final Map<MessageHandler, Consumer>    messageHandlerConsumers = new ConcurrentHashMap<MessageHandler,
			Consumer>();
	private final DelegatingConsumer<MessageEvent> delegatingConsumer      = new DelegatingConsumer<MessageEvent>();
	private final RingBatcher<MessageEvent> processor;

	private String beanName;

	/**
	 * Create a default multi-threaded producer channel.
	 */
	public ReactorSubscribableChannel() {
		this(false);
	}

	/**
	 * Create a {@literal ReactorSubscribableChannel} with a {@code ProducerType.SINGLE} if {@code
	 * singleThreadedProducer} is {@code true}, otherwise use {@code ProducerType.MULTI}.
	 *
	 * @param singleThreadedProducer
	 * 		whether to create a single-threaded producer or not
	 */
	public ReactorSubscribableChannel(boolean singleThreadedProducer) {
		this.beanName = String.format("%s@%s", getClass().getSimpleName(), ObjectUtils.getIdentityHexString(this));
		RingBatcherSpec<MessageEvent> spec = new RingBatcherSpec<MessageEvent>()
				.dataSupplier(new Supplier<MessageEvent>() {
					@Override
					public MessageEvent get() {
						return new MessageEvent();
					}
				})
				.consume(delegatingConsumer);
		if(singleThreadedProducer) {
			spec.singleThreadedProducer();
		} else {
			spec.multiThreadedProducer();
		}
		this.processor = spec.get();
	}

	@Override
	public void setBeanName(String beanName) {
		this.beanName = beanName;
	}

	public String getBeanName() {
		return beanName;
	}

	@Override
	public boolean subscribe(final MessageHandler handler) {
		Consumer<MessageEvent> consumer = new Consumer<MessageEvent>() {
			@Override
			public void accept(MessageEvent ev) {
				handler.handleMessage(ev.message);
			}
		};
		messageHandlerConsumers.put(handler, consumer);
		delegatingConsumer.add(consumer);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean unsubscribe(MessageHandler handler) {
		Consumer<MessageEvent> consumer = messageHandlerConsumers.remove(handler);
		if(null == consumer) {
			return false;
		}
		delegatingConsumer.remove(consumer);
		return true;
	}

	@Override
	public boolean send(Message<?> message) {
		return send(message, 0);
	}

	@Override
	public boolean send(Message<?> message, long timeout) {
		Operation<MessageEvent> op = processor.prepare();
		op.get().message = message;
		op.commit();
		return true;
	}

	private static class MessageEvent {
		Message<?> message;
	}

}
