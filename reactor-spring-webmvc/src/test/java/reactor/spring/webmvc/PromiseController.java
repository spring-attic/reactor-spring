package reactor.spring.webmvc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import reactor.core.Environment;
import reactor.core.Reactor;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.core.composable.spec.Promises;
import reactor.event.Event;

/**
 * @author Jon Brisbin
 */
@Controller
public class PromiseController {

	@Autowired
	Environment env;
	@Autowired
	Reactor     reactor;

	@RequestMapping("/promise")
	@ResponseBody
	public Promise<ResponseEntity<String>> get() {
		Deferred<ResponseEntity<String>, Promise<ResponseEntity<String>>> d = Promises.<ResponseEntity<String>>defer()
																																									.env(env)
																																									.get();

		reactor.notify("test", Event.wrap(d));

		return d.compose();
	}

}
