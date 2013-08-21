package reactor.spring.messaging;

import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.channel.AbstractSubscribableChannel;
import reactor.core.processor.Operation;
import reactor.core.processor.Processor;
import reactor.core.processor.spec.ProcessorSpec;
import reactor.function.Consumer;
import reactor.function.Supplier;
import reactor.function.support.DelegatingConsumer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Jon Brisbin
 */
public class ReactorSubscribableChannel extends AbstractSubscribableChannel {

	private final Map<MessageHandler, Consumer>    messageHandlerConsumers = new ConcurrentHashMap<MessageHandler, Consumer>();
	private final DelegatingConsumer<MessageEvent> delegatingConsumer      = new DelegatingConsumer<MessageEvent>();
	private final Processor<MessageEvent> processor;

	public ReactorSubscribableChannel() {
		this(false);
	}

	public ReactorSubscribableChannel(boolean singleThreadedProducer) {
		ProcessorSpec<MessageEvent> spec = new ProcessorSpec<MessageEvent>()
				.dataSupplier(new Supplier<MessageEvent>() {
					@Override
					public MessageEvent get() {
						return new MessageEvent();
					}
				})
				.consume(delegatingConsumer);
		if (singleThreadedProducer) {
			spec.singleThreadedProducer();
		} else {
			spec.multiThreadedProducer();
		}
		this.processor = spec.get();
	}

	@Override
	protected boolean sendInternal(Message<?> message, long timeout) {
		Operation<MessageEvent> op = processor.prepare();
		op.get().message = message;
		op.commit();
		return true;
	}

	@Override
	protected boolean hasSubscription(MessageHandler handler) {
		return messageHandlerConsumers.containsKey(handler);
	}

	@Override
	protected boolean subscribeInternal(final MessageHandler handler) {
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

	@Override
	protected boolean unsubscribeInternal(MessageHandler handler) {
		Consumer<MessageEvent> consumer = messageHandlerConsumers.remove(handler);
		if (null == consumer) {
			return false;
		}
		delegatingConsumer.remove(consumer);
		return true;
	}

	private static class MessageEvent {
		Message<?> message;
	}

}
