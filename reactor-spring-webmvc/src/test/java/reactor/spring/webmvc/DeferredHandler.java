package reactor.spring.webmvc;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import reactor.rx.Promise;
import reactor.spring.context.annotation.Consumer;
import reactor.spring.context.annotation.Selector;

/**
 * @author Jon Brisbin
 */
@Consumer
public class DeferredHandler {

	@Selector(value = "test", eventBus = "@eventBus")
	public void test(Promise<ResponseEntity<String>> d) {
		d.accept(new ResponseEntity<String>("Hello World!", HttpStatus.OK));
	}
}
