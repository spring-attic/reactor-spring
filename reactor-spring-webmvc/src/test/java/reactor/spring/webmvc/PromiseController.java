package reactor.spring.webmvc;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import reactor.bus.Event;
import reactor.bus.EventBus;
import reactor.rx.Promise;
import reactor.rx.Promises;


/**
 * @author Jon Brisbin
 * @author Stephane Maldini
 */
@Controller
public class PromiseController {

	@Autowired
	EventBus     reactor;

	@RequestMapping("/promise")
	public Promise<ResponseEntity<String>> get() {
        Promise<ResponseEntity<String>> d = Promises.<ResponseEntity<String>>ready();

		reactor.notify("test", Event.wrap(d));

		return d;
	}

}
