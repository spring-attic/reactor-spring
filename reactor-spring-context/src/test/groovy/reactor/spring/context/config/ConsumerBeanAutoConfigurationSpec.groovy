/*
 * Copyright (c) 2011-2015 Pivotal Software Inc., Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.spring.context.config

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import reactor.Environment
import reactor.bus.Event
import reactor.bus.EventBus
import reactor.spring.context.annotation.Consumer
import reactor.spring.context.annotation.Selector
import reactor.spring.context.annotation.SelectorType
import spock.lang.Specification

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
class ConsumerBeanAutoConfigurationSpec extends Specification {

	def "Annotated Consumer is wired to a Reactor"() {

		given:
		"an ApplicationContext with an annotated bean handler"
		def appCtx = new AnnotationConfigApplicationContext(AnnotatedHandlerConfig)
		def handlerBean = appCtx.getBean(HandlerBean)
		def reactor = appCtx.getBean(EventBus)

		when:
		"an Event is emitted onto the Reactor in context"
		reactor.notify('/a/b', Event.wrap("Hello World!"))

		then:
		"the method has been invoked"
		handlerBean.latch.await(1, TimeUnit.SECONDS)

	}

}

@Consumer
class HandlerBean {
	@Autowired
	EventBus eventBus
	def latch = new CountDownLatch(1)

	@Selector(value = '/{a}/{b}', type = SelectorType.URI)
	void handleTest(Event<String> ev) {
		//println "a=${ev.headers['a']}, b=${ev.headers['b']}"
		latch.countDown()
	}
}

@Configuration
class AnnotatedHandlerConfig {

	@Bean
	Environment env() {
		return new Environment()
	}

	@Bean
	EventBus eventBus(Environment env) {
		return env.rootBus
	}

	@Bean
	ConsumerBeanAutoConfiguration consumerBeanAutoConfiguration() {
		return new ConsumerBeanAutoConfiguration()
	}

	@Bean
	HandlerBean handlerBean() {
		return new HandlerBean()
	}

}
