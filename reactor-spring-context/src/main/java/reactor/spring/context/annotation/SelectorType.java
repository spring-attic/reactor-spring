package reactor.spring.context.annotation;

/**
 * Enum for indicating the type of {@link reactor.bus.selector.Selector} that should be created.
 *
 * @author Jon Brisbin
 */
public enum SelectorType {
	OBJECT, REGEX, URI, TYPE, JSON_PATH
}
