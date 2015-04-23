package reactor.spring.messaging.factory.net;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import reactor.Environment;
import reactor.core.support.Assert;
import reactor.fn.Consumer;
import reactor.fn.Function;
import reactor.io.codec.Codec;
import reactor.io.codec.DelimitedCodec;
import reactor.io.codec.LengthFieldCodec;
import reactor.io.codec.StandardCodecs;
import reactor.io.net.ChannelStream;
import reactor.io.net.NetStreams;
import reactor.io.net.Server;
import reactor.io.net.Spec;
import reactor.io.net.codec.syslog.SyslogCodec;
import reactor.io.net.http.HttpServer;
import reactor.io.net.impl.netty.tcp.NettyTcpServer;
import reactor.io.net.impl.netty.udp.NettyDatagramServer;
import reactor.io.net.tcp.TcpServer;
import reactor.io.net.udp.DatagramServer;

import java.net.InetSocketAddress;
import java.util.concurrent.locks.ReentrantLock;

/**
 * {@link org.springframework.beans.factory.FactoryBean} for creating a Reactor {@link reactor.io.net.Server}.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class NetServerFactoryBean<IN, OUT, CONN extends ChannelStream<IN, OUT>> implements FactoryBean<Server<IN,
		OUT, CONN>>, SmartLifecycle {

	private final ReentrantLock startLock = new ReentrantLock();
	private final Environment env;
	private volatile boolean started = false;

	private int     phase       = 0;
	private boolean autoStartup = true;

	private Class<? extends Server> serverImpl;
	private Server<IN, OUT, CONN>   server;
	private String                  dispatcher;

	private String host              = null;
	private int    port              = 3000;
	private Codec  codec             = StandardCodecs.BYTE_ARRAY_CODEC;
	private String framing           = "delimited";
	private String delimiter         = "LF";
	private int    lengthFieldLength = 4;
	private String transport         = "tcp";
	private MessageHandler messageHandler;

	public NetServerFactoryBean(Environment env) {
		this.env = env;
	}

	/**
	 * Set the name of the {@link reactor.core.Dispatcher} to use, which will be pulled from the current {@link
	 * reactor.Environment}.
	 *
	 * @param dispatcher dispatcher name
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setDispatcher(String dispatcher) {
		this.dispatcher = dispatcher;
		return this;
	}

	/**
	 * Set the phase in which this bean should start.
	 *
	 * @param phase the phase
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setPhase(int phase) {
		this.phase = phase;
		return this;
	}

	/**
	 * Set whether to perform auto startup.
	 *
	 * @param autoStartup {@code true} to enable auto startup, {@code false} otherwise
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setAutoStartup(boolean autoStartup) {
		this.autoStartup = autoStartup;
		return this;
	}

	/**
	 * Set the host to which this server will bind.
	 *
	 * @param host the host to bind to (defaults to {@code 0.0.0.0})
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setHost(String host) {
		Assert.notNull(host, "Host cannot be null.");
		this.host = host;
		return this;
	}

	/**
	 * Set the port to which this server will bind.
	 *
	 * @param port the port to bind to (defaults to {@code 3000})
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setPort(int port) {
		Assert.isTrue(port > 0, "Port must be greater than 0");
		this.port = port;
		return this;
	}

	/**
	 * Set the {@link reactor.io.codec.Codec} to use to managing encoding and decoding of the data.
	 * <p>
	 * The options for codecs currently are:
	 * <ul>
	 * <li>{@code bytes} - Use the standard byte array codec.</li>
	 * <li>{@code string} - Use the standard String codec.</li>
	 * <li>{@code syslog} - Use the standard Syslog codec.</li>
	 * </ul>
	 *
	 * @param codec the codec
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setCodec(String codec) {
		if ("bytes".equals(codec)) {
			this.codec = StandardCodecs.BYTE_ARRAY_CODEC;
		} else if ("string".equals(codec)) {
			this.codec = StandardCodecs.STRING_CODEC;
		} else if ("syslog".equals(codec)) {
			this.codec = new SyslogCodec();
		} else {
			throw new IllegalArgumentException("Codec '" + codec + "' not recognized.");
		}
		return this;
	}

	/**
	 * Set the type of framing to use.
	 * <p>
	 * The options for framing are:
	 * <ul>
	 * <li>{@code delimited} - Means use a delimited line codec (defaults to {@code LF}).</li>
	 * <li>{@code length} - Means use a length-field based codec where the initial bytes of a message are the length of
	 * the rest of the message.</li>
	 * </ul>
	 *
	 * @param framing type of framing
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setFraming(String framing) {
		Assert.isTrue("delimited".equals(framing) || "length".equals(framing));
		this.framing = framing;
		return this;
	}

	/**
	 * Set the single-byte delimiter to use when framing is set to 'delimited'.
	 * The options for delimiters are:
	 * <ul>
	 * <li>{@code LF} - Means use a line feed \\n.</li>
	 * <li>{@code CR} - Means use a carriage return \\r.</li>
	 * </ul>
	 *
	 * @param delimiter the delimiter to use
	 */
	public NetServerFactoryBean setDelimiter(String delimiter) {
		this.delimiter = delimiter;
		return this;
	}

	/**
	 * Set the length of the length field if using length-field framing.
	 *
	 * @param lengthFieldLength {@code 2} for a {@code short}, {@code 4} for an {@code int} (the default), or {@code 8}
	 *                          for a {@code long}
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setLengthFieldLength(int lengthFieldLength) {
		this.lengthFieldLength = lengthFieldLength;
		return this;
	}

	/**
	 * Set the transport to use for this {@literal NetServer}.
	 * <p>
	 * Options for transport currently are:
	 * <ul>
	 * <li>{@code tcp} - Use the built-in Netty TCP support.</li>
	 * <li>{@code udp} - Use the built-in Netty UDP support.</li>
	 * </ul>
	 *
	 * @param transport the transport to use
	 * @return {@literal this}
	 */
	public NetServerFactoryBean setTransport(String transport) {
		if ("tcp".equals(transport)) {
			this.serverImpl = NettyTcpServer.class;
		} else if ("udp".equals(transport)) {
			this.serverImpl = NettyDatagramServer.class;
		} else {
			throw new IllegalArgumentException("Transport must be either 'tcp' or 'udp'");
		}
		this.transport = transport;
		return this;
	}

	/**
	 * Set the {@link org.springframework.messaging.MessageHandler} that will handle each incoming message.
	 *
	 * @param messageHandler the {@link org.springframework.messaging.MessageHandler} to use
	 */
	public NetServerFactoryBean setMessageHandler(MessageHandler messageHandler) {
		this.messageHandler = messageHandler;
		return this;
	}

	@Override
	public boolean isAutoStartup() {
		return autoStartup;
	}

	@Override
	public void stop(Runnable callback) {
		startLock.lock();
		try {
			server.shutdown();
			started = false;
		} finally {
			startLock.unlock();
			if (null != callback) {
				callback.run();
			}
		}
	}

	@Override
	public void start() {
		startLock.lock();
		try {
			server.start();
			started = true;
		} finally {
			startLock.unlock();
		}
	}

	@Override
	public void stop() {
		stop(null);
	}

	@Override
	public boolean isRunning() {
		startLock.lock();
		try {
			return started;
		} finally {
			startLock.unlock();
		}
	}

	@Override
	public int getPhase() {
		return phase;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Server<IN, OUT, CONN> getObject() throws Exception {
		if (null == server) {
			final InetSocketAddress bindAddress;
			if (null == host) {
				bindAddress = InetSocketAddress.createUnresolved("0.0.0.0", port);
			} else {
				bindAddress = new InetSocketAddress(host, port);
			}

			final Codec framedCodec;
			if ("delimited".equals(framing)) {
				if ("LF".equals(delimiter)) {
					framedCodec = new DelimitedCodec(this.codec);
				} else if ("CR".equals(delimiter)) {
					framedCodec = new DelimitedCodec((byte) '\r', true, this.codec);
				} else {
					framedCodec = codec;
				}
			} else if ("length".equals(framing)) {
				framedCodec = new LengthFieldCodec(lengthFieldLength, this.codec);
			} else {
				framedCodec = codec;
			}

			final Function<Spec.ServerSpec<IN, OUT, CONN, ?, ?>, Spec.ServerSpec<IN, OUT, CONN, ?, ?>> commonSpec =
					new Function<Spec.ServerSpec<IN, OUT, CONN, ?, ?>, Spec.ServerSpec<IN, OUT, CONN, ?, ?>>() {

						@Override
						public Spec.ServerSpec<IN, OUT, CONN, ?, ?> apply(Spec.ServerSpec<IN, OUT, CONN, ?, ?> s) {
							if (dispatcher != null) {
								s.dispatcher(dispatcher);
							}
							return s.env(env).codec(framedCodec).listen(bindAddress);
						}
					};

			if ("tcp".equals(transport)) {
				server = (Server<IN, OUT, CONN>) NetStreams.<IN, OUT>tcpServer(
						null == serverImpl
								? NettyTcpServer.class
								: (Class<? extends TcpServer>) serverImpl,
						new Function<Spec.TcpServerSpec<IN, OUT>, Spec.TcpServerSpec<IN, OUT>>() {
							@Override
							public Spec.TcpServerSpec<IN, OUT> apply(Spec.TcpServerSpec<IN, OUT> spec) {
								commonSpec.apply((Spec.ServerSpec<IN, OUT, CONN, ?, ?>) spec);
								return spec;
							}
						});
			} else if ("udp".equals(transport)) {
				server = (Server<IN, OUT, CONN>) NetStreams.<IN, OUT>udpServer(
						null == serverImpl
								? DatagramServer.class
								: (Class<? extends DatagramServer>) serverImpl,
						new Function<Spec.DatagramServerSpec<IN, OUT>, Spec.DatagramServerSpec<IN, OUT>>() {
							@Override
							public Spec.DatagramServerSpec<IN, OUT> apply(Spec.DatagramServerSpec<IN, OUT> spec) {
								commonSpec.apply((Spec.ServerSpec<IN, OUT, CONN, ?, ?>) spec);
								return spec;
							}
						});
			} else if ("http".equals(transport)) {
				server = (Server<IN, OUT, CONN>) NetStreams.<IN, OUT>httpServer(
						null == serverImpl
								? HttpServer.class
								: (Class<? extends HttpServer>) serverImpl,
						new Function<Spec.HttpServerSpec<IN, OUT>, Spec.HttpServerSpec<IN, OUT>>() {
							@Override
							public Spec.HttpServerSpec<IN, OUT> apply(Spec.HttpServerSpec<IN, OUT> spec) {
								commonSpec.apply((Spec.ServerSpec<IN, OUT, CONN, ?, ?>) spec);
								return spec;
							}
						});
			} else {
				throw new IllegalArgumentException(transport + " not recognized as a valid transport type.");
			}

			if (server != null) {
				server.subscribe(new Subscriber<CONN>() {
					Subscription s;

					@Override
					public void onSubscribe(Subscription s) {
						s.request(Long.MAX_VALUE);
						this.s = s;
					}

					@Override
					public void onNext(CONN ch) {
						ch.consume(new Consumer<IN>() {
							@Override
							public void accept(IN o) {
								if (null == messageHandler) {
									return;
								}
								Message<IN> msg = new GenericMessage<IN>(o);
								messageHandler.handleMessage(msg);
							}
						});
					}

					@Override
					public void onError(Throwable t) {
						if (s != null) {
							s.cancel();
						}
					}

					@Override
					public void onComplete() {
						if (s != null) {
							s.cancel();
						}
					}
				});
			}
		}
		return server;
	}

	@Override
	public Class<?> getObjectType() {
		return Server.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
