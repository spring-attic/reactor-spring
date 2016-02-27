package reactor.spring.selector

import reactor.bus.Event
import reactor.bus.EventBus
import spock.lang.Specification

import java.util.function.Consumer

import static ExpressionSelector.E

/**
 * @author Jon Brisbin
 */
class ExpressionSelectorSpec extends Specification {


	def "SpEL Expressions can be used as Selectors"() {

		given:
			"a plain Reactor"
			def r = EventBus.config().get()
			def names = []

		when:
			"a SpEL expression is used as a Selector and the Reactor is notified"
			r.on(E("name == 'John Doe'"), { ev -> names << ev.key.name } as Consumer<Event<TestBean>>)
			r.notify(new TestBean(name: "Jane Doe"))
			r.notify(new TestBean(name: "Jim Doe"))
			r.notify(new TestBean(name: "John Doe"))

		then:
			"only one should have matched"
			names == ["John Doe"]

	}

}

class TestBean {
	String name
}
