package reactor.spring.context.config;

import org.springframework.context.annotation.Import;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Helper annotation to be placed on {@link org.springframework.context.annotation.Configuration} classes to ensure
 * a {@link reactor.fn.timer.Timer} and {@link org.reactivestreams.Processor} are
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
	 * The bean name of {@link reactor.fn.Supplier} that can provide an instance (or instances) of
	 *  {@link reactor.fn.timer.Timer} to be registered in the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return bean name of {@link reactor.fn.timer.Timer} {@link reactor.fn.Supplier}
	 */
	String timerSupplier() default "";

	/**
	 * The bean name of {@link reactor.fn.Supplier} that can provide an instance (or instances) of
	 *  {@link org.reactivestreams.Processor} to be registered in the {@link org.springframework.context.ApplicationContext}.
	 *
	 * @return bean name of {@link org.reactivestreams.Processor} {@link reactor.fn.Supplier}
	 */
	String processorSupplier() default "";

}
