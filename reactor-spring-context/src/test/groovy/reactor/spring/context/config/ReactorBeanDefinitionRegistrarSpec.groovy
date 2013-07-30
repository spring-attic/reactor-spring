package reactor.spring.context.config

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.stereotype.Component
import reactor.core.Environment
import reactor.core.Reactor
import reactor.core.spec.Reactors
import reactor.event.Event
import reactor.spring.context.annotation.On
import spock.lang.Specification

/**
 * @author Jon Brisbin
 */
class ReactorBeanDefinitionRegistrarSpec extends Specification {

  def "EnableReactor annotation causes default components to be created"() {

    given:
      "an annotated configuration"
      def appCtx = new AnnotationConfigApplicationContext(ReactorConfig)
      def consumer = appCtx.getBean(LoggingConsumer)
      def reactor = appCtx.getBean(Reactor)

    when:
      "notifying the injected Reactor"
      reactor.notify("test", Event.wrap("World"))

    then:
      "the method will have been invoked"
      consumer.count == 1

  }

}

@Component
class LoggingConsumer {
  @Autowired Reactor reactor
  long count = 0
  Logger log = LoggerFactory.getLogger(LoggingConsumer)

  @On("test") void onTest(String s) {
    log.info("Hello ${s}!")
    count++
  }
}

@Configuration
@EnableReactor
@ComponentScan
class ReactorConfig {

  @Bean Reactor reactor(Environment env) {
    Reactors.reactor().env(env).synchronousDispatcher().get()
  }

}
