package reactor.spring.context.config;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.context.annotation.Import;

/**
 * Helper annotation to be placed on {@link org.springframework.context.annotation.Configuration} classes to ensure
 * a {@link reactor.core.scheduler.TimedScheduler} and
 * {@link org.reactivestreams.Processor} are
 * created in application context as well as create the necessary beans for
 * automatic wiring of annotated beans.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Import(ReactorBeanDefinitionRegistrar.class)
public @interface EnableReactor {

	/**
	 * The bean name of {@link java.util.function.Supplier} that can provide an instance (or instances) of
	 *  {@link reactor.core.scheduler.TimedScheduler} to be registered in the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return bean name of {@link reactor.core.scheduler.TimedScheduler} {@link java.util.function.Supplier}
	 */
	String timerSupplier() default "";

	/**
	 * The bean name of {@link java.util.function.Supplier} that can provide an instance (or instances) of
	 *  {@link org.reactivestreams.Processor} to be registered in the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return bean name of {@link org.reactivestreams.Processor} {@link java.util.function.Supplier}
	 */
	String processorSupplier() default "";

}
