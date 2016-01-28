package reactor.spring.context.config;

import java.util.Map;

import org.reactivestreams.Processor;
import reactor.core.timer.Timer;
import reactor.core.util.PlatformDependent;
import reactor.fn.Supplier;
import reactor.spring.factory.CreateOrReuseFactoryBean;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.util.StringUtils;

/**
 * {@link ImportBeanDefinitionRegistrar} implementation that configures necessary Reactor components.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
public class ReactorBeanDefinitionRegistrar implements ImportBeanDefinitionRegistrar {

	private static final String DEFAULT_TIMER_SUPPLIER_NAME     = "reactorTimer";
	private static final String DEFAULT_PROCESSOR_SUPPLIER_NAME = "reactorProcessor";

	private static final Supplier<Supplier<? extends Processor>> DEFAULT_PROCESSOR_SUPPLIER = new Supplier<Supplier<?
	  extends Processor>>() {
		@Override
		public Supplier<? extends Processor> get() {
			return ProcessorGroup.async("reactor-spring",
			  PlatformDependent.MEDIUM_BUFFER_SIZE
			);
		}
	};

	private static final Supplier<Supplier<? extends Timer>> DEFAULT_TIMER_SUPPLIER = new Supplier<Supplier<?
	  extends Timer>>() {
		@Override
		public Supplier<? extends Timer> get() {
			return new Supplier<Timer>() {
				@Override
				public Timer get() {
					return Timer.create();
				}
			};
		}
	};

	protected <T> void registerReactorBean(BeanDefinitionRegistry registry,
	                                       String attrValue,
	                                       String name,
	                                       Class<T> tClass,
	                                       Supplier<Supplier<? extends T>> supplier) {

		// Create a root Enivronment
		if (!registry.containsBeanDefinition(name)) {
			BeanDefinitionBuilder envBeanDef = BeanDefinitionBuilder.rootBeanDefinition(CreateOrReuseFactoryBean
			  .class);
			envBeanDef.addConstructorArgValue(name);
			envBeanDef.addConstructorArgValue(tClass);

			if (StringUtils.hasText(attrValue)) {
				envBeanDef.addConstructorArgReference(attrValue);
			} else {
				envBeanDef.addConstructorArgValue(supplier.get());
			}
			registry.registerBeanDefinition(name, envBeanDef.getBeanDefinition());
		}
	}

	@Override
	public void registerBeanDefinitions(AnnotationMetadata meta, BeanDefinitionRegistry registry) {
		Map<String, Object> attrs = meta.getAnnotationAttributes(EnableReactor.class.getName());

		registerReactorBean(registry,
		  (String) attrs.get("timerSupplier"),
		  DEFAULT_TIMER_SUPPLIER_NAME,
		  Timer.class,
		  DEFAULT_TIMER_SUPPLIER
		);


		registerReactorBean(registry,
		  (String) attrs.get("processorSupplier"),
		  DEFAULT_PROCESSOR_SUPPLIER_NAME,
		  Processor.class,
		  DEFAULT_PROCESSOR_SUPPLIER
		);


		// Create a ConsumerBeanAutoConfiguration
		if (!registry.containsBeanDefinition(ConsumerBeanAutoConfiguration.class.getName())) {
			BeanDefinitionBuilder autoConfigDef = BeanDefinitionBuilder.rootBeanDefinition
			  (ConsumerBeanAutoConfiguration.class);
			registry.registerBeanDefinition(ConsumerBeanAutoConfiguration.class.getName(), autoConfigDef
			  .getBeanDefinition());
		}
	}
}
