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
package reactor.spring.context.annotation;

import java.lang.annotation.*;

/**
 * Placed on a method to denote that it is an event handler.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface Selector {

	/**
	 * An expression that evaluates to a {@link reactor.bus.selector.Selector} to register
     * this handler with the {@link
	 * reactor.bus.EventBus}.
	 * If empty, consumer will be subscribed on the global reactor selector
	 * {@link reactor.bus.EventBus#on(reactor.bus.selector.Selector selector, reactor.fn.Consumer)}
	 *
	 * @return An expression to be evaluated.
	 */
	String value() default "";

	/**
	 * An expression that evaluates to the {@link reactor.bus.EventBus} on which to place this handler.
	 *
	 * @return An expression to be evaluated.
	 */
	String eventBus() default "eventBus";

	/**
	 * The type of {@link reactor.bus.selector.Selector} to register.
	 *
	 * @return The type of the {@link reactor.bus.selector.Selector}.
	 */
	SelectorType type() default SelectorType.OBJECT;

}
