package reactor.spring.messaging;

import org.reactivestreams.Processor;
import org.springframework.beans.factory.BeanNameAware;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.ObjectUtils;
import reactor.core.processor.RingBufferProcessor;
import reactor.fn.Consumer;
import reactor.rx.Streams;
import reactor.rx.subscriber.Control;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Subscribable {@link org.springframework.messaging.MessageChannel} implementation that uses the RinBuffer-based
 * Reactor {@link reactor.core.processor.RingBufferProcessor} to publish messages for efficiency at high volumes.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ReactorSubscribableChannel implements BeanNameAware, MessageChannel, SubscribableChannel {

	private final Map<MessageHandler, Control>
			messageHandlerConsumers =
			new ConcurrentHashMap<MessageHandler, Control>();

	private final Processor<Message<?>, Message<?>> processor;

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
	 * @param singleThreadedProducer whether to create a single-threaded producer or not
	 */
	public ReactorSubscribableChannel(boolean singleThreadedProducer) {
		this.beanName = String.format("%s@%s", getClass().getSimpleName(), ObjectUtils.getIdentityHexString(this));
		if (singleThreadedProducer) {
			this.processor = RingBufferProcessor.create();
		} else {
			this.processor = RingBufferProcessor.share();
		}
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
		Consumer<Message<?>> consumer = new Consumer<Message<?>>() {
			@Override
			public void accept(Message<?> ev) {
				handler.handleMessage(ev);
			}
		};
		Control c = Streams.from(processor).consume(consumer);
		messageHandlerConsumers.put(handler, c);

		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean unsubscribe(MessageHandler handler) {
		Control control = messageHandlerConsumers.remove(handler);
		if (null == control) {
			return false;
		}
		control.cancel();
		return true;
	}

	@Override
	public boolean send(Message<?> message) {
		return send(message, 0);
	}

	@Override
	public boolean send(Message<?> message, long timeout) {
		processor.onNext(message);
		return true;
	}

}
