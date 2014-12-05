package reactor.spring.context.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.core.Environment
import reactor.event.Event
import reactor.event.EventBus
import reactor.spring.context.annotation.ReplyTo
import reactor.spring.context.annotation.Selector
import spock.lang.Specification

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
			reactor.notify("test", Event.wrap("World"))

		then:
			"the method will have been invoked"
			consumer.count == 2

		when:
			"notifying the injected Reactor with replyTo header"
			reactor.notify("test2", Event.wrap("World").setReplyTo("reply2"))

		then:
			"the method will have been invoked"
			consumer.count == 4

	}

	def "ReplyTo annotation causes replies to be handled"() {

		given:
			'an annotated configuration'
			def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
			def consumer = appCtx.getBean(LoggingConsumer)
			def reactor = appCtx.getBean(EventBus)

		when:
			"notifying the injected Reactor"
			reactor.notify("test", Event.wrap("World"))

		then:
			"the method will have been invoked"
			consumer.count == 2

	}

}

@reactor.spring.context.annotation.Consumer
class LoggingConsumer {
	@Autowired
	EventBus eventBus
	long count = 0
	Logger log = LoggerFactory.getLogger(LoggingConsumer)

	@Selector("test2")
	@ReplyTo
	String onTest2(String s) {
		log.info("Hello again ${s}!")
		"Goodbye again ${s}!"
		count++
	}

	@Selector("reply2")
	void onReply2(String s) {
		log.info("Got reply: ${s}")
		count++
	}

	@Selector("test")
	@ReplyTo("reply")
	String onTest(String s) {
		log.info("Hello ${s}!")
		count++
		"Goodbye ${s}!"
	}

	@Selector("reply")
	void onReply(String s) {
		log.info("Got reply: ${s}")
		count++
	}
}

@Configuration
@EnableReactor("default")
public class ReactorConfig {

	@Bean
	EventBus eventBus(Environment env) {
		EventBus.create(env, "sync")
	}

	@Bean
	LoggingConsumer loggingConsumer() {
		new LoggingConsumer()
	}
	
}