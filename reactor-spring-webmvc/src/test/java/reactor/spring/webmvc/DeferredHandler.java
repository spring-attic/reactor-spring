package reactor.spring.webmvc;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import reactor.core.composable.Deferred;
import reactor.core.composable.Promise;
import reactor.spring.annotation.Consumer;
import reactor.spring.annotation.Selector;

/**
 * @author Jon Brisbin
 */
@Consumer
@Component
public class DeferredHandler {

	@Selector(value = "test", reactor = "@reactor")
	public void test(Deferred<ResponseEntity<String>, Promise<ResponseEntity<String>>> d) {
		d.accept(new ResponseEntity<String>("Hello World!", HttpStatus.OK));
	}

}
