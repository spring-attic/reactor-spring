package reactor.spring.messaging;

import reactor.jarjar.com.lmax.disruptor.BlockingWaitStrategy;
import reactor.jarjar.com.lmax.disruptor.EventFactory;
import reactor.jarjar.com.lmax.disruptor.RingBuffer;
import reactor.jarjar.com.lmax.disruptor.WaitStrategy;
import reactor.jarjar.com.lmax.disruptor.dsl.ProducerType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * A {@link org.springframework.messaging.MessageHandler} implementation that buffers message payloads until
 * an internal {@link com.lmax.disruptor.RingBuffer} is filled. When the buffer is full, a new aggregate
 * {@link org.springframework.messaging.Message} is created and sent into the delegate
 * {@link org.springframework.messaging.MessageHandler}.
 *
 * @author Jon Brisbin
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class RingBufferBatchingMessageHandler implements MessageHandler, InitializingBean {

	private static final AtomicLongFieldUpdater<RingBufferBatchingMessageHandler>                      SEQ_START =
			AtomicLongFieldUpdater.newUpdater(RingBufferBatchingMessageHandler.class, "sequenceStart");
	private static final AtomicReferenceFieldUpdater<RingBufferBatchingMessageHandler, MessageHeaders> MSG_HDRS  =
			AtomicReferenceFieldUpdater.newUpdater(RingBufferBatchingMessageHandler.class, MessageHeaders.class, "messageHeaders");

	private final List messagePayloads = new ArrayList();

	private final MessageHandler delegate;
	private final int            batchSize;
	private final RingBuffer     ringBuffer;

	private volatile MessageHeaders messageHeaders;
	private volatile long sequenceStart = -1;

	/**
	 * Create a batching handler using the given delegate and {@code batchSize}.
	 *
	 * @param delegate
	 * 		the delegate {@link org.springframework.messaging.MessageHandler} to invoke
	 * @param batchSize
	 * 		the size of the batch
	 */
	public RingBufferBatchingMessageHandler(MessageHandler delegate, int batchSize) {
		this(delegate,
		     batchSize,
		     ProducerType.MULTI,
		     new BlockingWaitStrategy());
	}

	/**
	 * Create a batching handler using the given delegate, {@code batchSize}, {@link com.lmax.disruptor.dsl
	 * .ProducerType}, and {@link com.lmax.disruptor.WaitStrategy}.
	 *
	 * @param delegate
	 * 		the delegate {@link org.springframework.messaging.MessageHandler} to invoke
	 * @param batchSize
	 * 		the size of the batch
	 * @param producerType
	 * 		the {@literal ProducerType}
	 * @param waitStrategy
	 * 		the {@literal WaitStrategy}
	 */
	public RingBufferBatchingMessageHandler(MessageHandler delegate,
	                                        int batchSize,
	                                        ProducerType producerType,
	                                        WaitStrategy waitStrategy) {
		this.delegate = delegate;
		this.batchSize = batchSize;
		this.ringBuffer = RingBuffer.create(
				producerType,
				new EventFactory() {
					@Override
					public Object newInstance() {
						return new Object();
					}
				},
				batchSize,
				waitStrategy
		);
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Assert.notNull(delegate, "");
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleMessage(Message<?> message) throws MessagingException {
		// get sequence id
		long seqId = ringBuffer.next();
		// if this is the first message of the batch
		if(null == MSG_HDRS.get(this)) {
			// save the start id
			if(SEQ_START.compareAndSet(this, -1, sequenceStart)) {
				// pull the headers from the first message
				MSG_HDRS.compareAndSet(this, null, message.getHeaders());
			}
		}
		// add all payloads
		messagePayloads.add(message.getPayload());

		// ring buffer is now full
		if(seqId % batchSize == 0) {
			Message<?> msg = new GenericMessage<Object>(new ArrayList<Object>(messagePayloads), messageHeaders);
			// dispatch batched message
			delegate.handleMessage(msg);
			// clear payloads and headers
			messagePayloads.clear();
			MSG_HDRS.compareAndSet(this, messageHeaders, null);
			// reset ring buffer
			long start = sequenceStart;
			ringBuffer.publish(start, seqId);
			SEQ_START.compareAndSet(this, sequenceStart, -1);
		}
	}

}
