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
package reactor.spring.context.annotation;

import java.lang.annotation.*;

/**
 * Indicate a method return is to be sent to the key referenced by the given expression.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@Target({ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ReplyTo {

	/**
	 * An expression which evaluates to a key to which is sent the method return value.
	 * If empty, consumer will try to use {@link reactor.bus.Event#getReplyTo()} header.
	 *
	 * @return The expression.
	 */
	String value() default "";

}
