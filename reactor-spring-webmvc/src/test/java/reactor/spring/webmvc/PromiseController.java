package reactor.spring.webmvc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.Environment;
import reactor.event.Event;
import reactor.event.EventBus;
import reactor.rx.Promise;
import reactor.rx.Promises;


/**
 * @author Jon Brisbin
 */
@Controller
public class PromiseController {

	@Autowired
	Environment env;
	@Autowired
	EventBus     reactor;

	@RequestMapping("/promise")
	public Promise<ResponseEntity<String>> get() {
        Promise<ResponseEntity<String>> d = Promises.<ResponseEntity<String>>prepare(env);

		reactor.notify("test", Event.wrap(d));

		return d;
	}

}
