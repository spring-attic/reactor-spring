package reactor.spring.context.config

import org.reactivestreams.Processor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.spring.context.annotation.Consumer
import reactor.spring.context.annotation.ReplyTo
import reactor.spring.context.annotation.Selector
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class ReactorBeanDefinitionRegistrarSpec extends Specification {

	def "EnableReactor annotation causes default components to be created"() {

		given:
			"an annotated configuration"
			def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
			def consumer = appCtx.getBean(LoggingConsumer)
			def reactor = appCtx.getBean(EventBus)

		when:
			"notifying the injected Reactor"
		  def latch = consumer.reset(2)
			reactor.notify("test", Event.wrap("World"))

		then:
			"the method will have been invoked"
			latch.await(3,  TimeUnit.SECONDS)

		when:
			"notifying the injected Reactor with replyTo header"
			latch = consumer.reset(2)
			reactor.notify("test2", Event.wrap("World").setReplyTo("reply2"))

		then:
			"the method will have been invoked"
			latch.await(3,  TimeUnit.SECONDS)

	}

	def "ReplyTo annotation causes replies to be handled"() {

		given:
			'an annotated configuration'
			def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
			def consumer = appCtx.getBean(LoggingConsumer)
			def reactor = appCtx.getBean(EventBus)

		when:
			"notifying the injected Reactor"
			def latch = consumer.reset(2)
			reactor.notify("test", Event.wrap("World"))

		then:
			"the method will have been invoked"
			latch.await(3,  TimeUnit.SECONDS)

	}

}

@Consumer
class LoggingConsumer {
	@Autowired
	EventBus eventBus
	Logger log = LoggerFactory.getLogger(LoggingConsumer)

	CountDownLatch latch

	CountDownLatch reset(int count){
		latch = new CountDownLatch(count)
	}

	@Selector("test2")
	@ReplyTo
	String onTest2(String s) {
		log.info("Hello again ${s}!")
		latch?.countDown()
		"Goodbye again ${s}!"
	}

	@Selector("reply2")
	void onReply2(String s) {
		log.info("Got reply: ${s}")
		latch?.countDown()
	}

	@Selector("test")
	@ReplyTo("reply")
	String onTest(String s) {
		log.info("Hello ${s}!")
		latch?.countDown()
		"Goodbye ${s}!"
	}

	@Selector("reply")
	void onReply(String s) {
		log.info("Got reply: ${s}")
		latch?.countDown()
	}
}

@Configuration
@EnableReactor
public class ReactorConfig {

	@Bean
	EventBus eventBus(Processor<Event<?>, Event<?>> processor) {
		EventBus.create(processor)
	}

	@Bean
	LoggingConsumer loggingConsumer() {
		new LoggingConsumer()
	}
	
}