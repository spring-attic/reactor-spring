/*
 * Copyright (c) 2011-2016 Pivotal Software Inc., Inc. All Rights Reserved.
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

package reactor.spring.task

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.test.context.ContextConfiguration
import reactor.core.support.wait.YieldingWaitStrategy
import reactor.spring.core.task.WorkQueueAsyncTaskExecutor
import spock.lang.Ignore
import spock.lang.Specification

import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@ContextConfiguration
class TaskExecutorsSpec extends Specification {


	@Autowired
	WorkQueueAsyncTaskExecutor workQueue

	def "Work queue executor executes tasks"() {

		when: "a task is submitted"
			def latch = new CountDownLatch(1)
			workQueue.execute({
				latch.countDown()
			})

		then: "latch was counted down"
			latch.await(1, TimeUnit.SECONDS)

		when: "a value-returning task is submitted"
			def f = workQueue.submit({
																 return "Hello World!"
															 } as Callable)

		then: "the Future blocks until completion and the value is returned"
			f.get(1, TimeUnit.SECONDS) == "Hello World!"

	}

	@Ignore
	def "Work queue executor is performant"() {

		when: "a Closure is submitted"
			def count = 0
			def start = System.currentTimeMillis()
			def counter = { count++ }
			(1..1000).each {
				workQueue.execute counter
			}
			workQueue.stop()
			def end = System.currentTimeMillis()
			def elapsed = end - start
			int throughput = count / (elapsed / 1000)
			println "throughput: $throughput"

		then:
			// really small just to make sure it passes on CI servers
			throughput > 1000

	}

	@Configuration
	static class TestConfig {

		@Bean
		WorkQueueAsyncTaskExecutor workQueueAsyncTaskExecutor() {
			def ex = new WorkQueueAsyncTaskExecutor()
			ex.shared = false
			ex.waitStrategy = new YieldingWaitStrategy()
			return ex
		}

	}

}
