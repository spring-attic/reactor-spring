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

package reactor.spring.factory;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.util.Assert;
import reactor.Environment;
import reactor.event.EventBus;
import reactor.event.spec.EventBusSpec;

/**
 * A Spring {@link FactoryBean} for creating a singleton {@link EventBus}.
 *
 * @author Jon Brisbin
 */
public class EventBusFactoryBean implements FactoryBean<EventBus> {

	private volatile EventBus eventBus;

	/**
	 * Creates a new EventBusFactoryBean that will use the given environment when configuring
	 * and creating its EventBus. The EventBus will use a synchronous dispatcher and broadcast
	 * event routing.
	 *
	 * @param env
	 * 		The environment to use
	 */
	public EventBusFactoryBean(Environment env) {
		this(env, null, null);
	}

	/**
	 * Creates a new EventBusFactoryBean that will use the given environment when configuring
	 * and creating its EventBus and will configure the reactor with the dispatcher with
	 * the given name found in the environment. The EventBus will use broadcast event routing.
	 *
	 * @param env
	 * 		The environment to use
	 * @param dispatcher
	 * 		The dispatcher to configure the EventBus with
	 */
	public EventBusFactoryBean(Environment env,
	                           String dispatcher) {
		this(env, dispatcher, null);
	}

	/**
	 * Creates a new EventBusFactoryBean that will use the given environment when configuring
	 * and creating its EventBus and will configure the reactor with the dispatcher with
	 * the given name found in the environment. The EventBus will use the given
	 * {@code eventRouting}.
	 *
	 * @param env
	 * 		The environment to use
	 * @param dispatcher
	 * 		The dispatcher to configure the EventBus with
	 * @param eventRouting
	 * 		The type of event routing to use
	 */
	public EventBusFactoryBean(Environment env,
	                           String dispatcher,
	                           EventRouting eventRouting) {
		Assert.notNull(env, "Environment cannot be null.");

		EventBusSpec spec = EventBus.config().env(env);
		if(null != dispatcher) {
			if("sync".equals(dispatcher)) {
				spec.synchronousDispatcher();
			} else {
				spec.dispatcher(dispatcher);
			}
		}
		if(null != eventRouting) {
			switch(eventRouting) {
				case BROADCAST_EVENT_ROUTING:
					spec.broadcastEventRouting();
					break;
				case RANDOM_EVENT_ROUTING:
					spec.randomEventRouting();
					break;
				case ROUND_ROBIN_EVENT_ROUTING:
					spec.roundRobinEventRouting();
					break;
			}
		}
		this.eventBus = spec.get();
	}

	@Override
	public EventBus getObject() throws Exception {
		return eventBus;
	}

	@Override
	public Class<?> getObjectType() {
		return EventBus.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}

}
