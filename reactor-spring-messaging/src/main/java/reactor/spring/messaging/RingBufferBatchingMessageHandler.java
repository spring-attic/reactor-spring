package reactor.spring.messaging;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.ProducerType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link org.springframework.messaging.MessageHandler} implementation that buffers message payloads until
 * an internal {@link com.lmax.disruptor.RingBuffer} is filled. When the buffer is full, a new aggregate
 * {@link org.springframework.messaging.Message} is created and sent into the delegate
 * {@link org.springframework.messaging.MessageHandler}.
 *
 * @author Jon Brisbin
 */
public class RingBufferBatchingMessageHandler implements MessageHandler, InitializingBean {

	private final List messagePayloads = new ArrayList();

	private final MessageHandler delegate;
	private final int            batchSize;
	private final RingBuffer     ringBuffer;

	private MessageHeaders messageHeaders;
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
		if(null == messageHeaders) {
			// save the start id
			sequenceStart = seqId;
			// pull the headers from the first message
			messageHeaders = message.getHeaders();
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
			messageHeaders = null;
			// reset ring buffer
			long start = sequenceStart;
			ringBuffer.publish(start, seqId);
			sequenceStart = -1;
		}
	}

}
