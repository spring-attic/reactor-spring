package reactor.spring.messaging;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import reactor.core.Cancellation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.TopicProcessor;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.SubscribableChannel;
import org.springframework.util.ObjectUtils;

/**
 * Subscribable {@link org.springframework.messaging.MessageChannel} implementation that uses the RinBuffer-based
 * Reactor {@link reactor.core.publisher.TopicProcessor} to publish messages for efficiency at high volumes.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ReactorSubscribableChannel implements BeanNameAware, MessageChannel, SubscribableChannel {

	private final Map<MessageHandler, Cancellation>
			messageHandlerConsumers =
			new ConcurrentHashMap<>();

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
			this.processor = TopicProcessor.create();
		} else {
			this.processor = TopicProcessor.share();
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
		Consumer<Message<?>> consumer = handler::handleMessage;
		Cancellation c = Flux.from(processor).subscribe(consumer);
		messageHandlerConsumers.put(handler, c);

		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean unsubscribe(MessageHandler handler) {
		Cancellation control = messageHandlerConsumers.remove(handler);
		if (null == control) {
			return false;
		}
		control.dispose();
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
