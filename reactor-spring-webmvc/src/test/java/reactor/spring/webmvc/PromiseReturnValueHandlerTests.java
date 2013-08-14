package reactor.spring.webmvc;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.spec.Reactors;
import reactor.spring.context.config.EnableReactor;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * @author Jon Brisbin
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
@ContextConfiguration
public class PromiseReturnValueHandlerTests {

	@Autowired
	private WebApplicationContext wac;
	private MockMvc               mvc;

	@Before
	public void setup() {
		mvc = MockMvcBuilders.webAppContextSetup(wac).build();
	}

	@Test
	public void promiseReturnValueHandlerAwaitsValues() throws Exception {
		mvc.perform(get("/promise"))
			 .andExpect(status().isOk())
			 .andExpect(content().string("Hello World!"));
	}

	@Configuration
	@EnableReactor
	@ComponentScan
	static class ContextConfig extends WebMvcConfigurationSupport {

		@Bean
		public Reactor reactor(Environment env) {
			return Reactors.reactor().env(env).get();
		}

		@Bean
		public PromiseHandlerMethodReturnValueHandler promiseHandlerMethodReturnValueHandler(RequestMappingHandlerAdapter adapter) {
			List<HandlerMethodReturnValueHandler> returnValueHandlers = adapter.getReturnValueHandlers();

			List<HandlerMethodReturnValueHandler> newReturnValueHandlers = new ArrayList<HandlerMethodReturnValueHandler>();

			PromiseHandlerMethodReturnValueHandler phmrvh = new PromiseHandlerMethodReturnValueHandler(returnValueHandlers);
			newReturnValueHandlers.add(phmrvh);
			newReturnValueHandlers.addAll(returnValueHandlers);

			adapter.setReturnValueHandlers(newReturnValueHandlers);

			return phmrvh;
		}

	}

}
