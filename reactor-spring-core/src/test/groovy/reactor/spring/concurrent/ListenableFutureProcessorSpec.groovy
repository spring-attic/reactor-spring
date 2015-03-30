package reactor.spring.concurrent

import org.springframework.util.concurrent.FailureCallback
import org.springframework.util.concurrent.SuccessCallback
import reactor.rx.broadcast.Broadcaster
import reactor.spring.core.concurrent.AdaptingListenableFutureProcessor
import reactor.spring.core.concurrent.ListenableFutureProcessor
import spock.lang.Specification

/**
 * Created by jbrisbin on 3/30/15.
 */
class ListenableFutureProcessorSpec extends Specification {

  def "ListenableFutureProcessor listens for values"() {

    given:
      def val = ""
      def f = new ListenableFutureProcessor()
      f.addCallback({ v -> val = v } as SuccessCallback, null)
      def b = Broadcaster.create()
      b.subscribe(f)

    when: 'a value is pushed downstream'
      b.onNext("Hello World!")

    then: 'the value is available'
      f.get() == 'Hello World!'

  }

  def "AdaptingListenableFutureProcessor adapts values"() {

    given:
      int val = 0
      def f = new AdaptingListenableFutureProcessor<String, Integer>() {
        @Override
        protected Integer adapt(String s) {
          return Integer.parseInt(s)
        }
      }
      f.addCallback({ v -> val = v } as SuccessCallback, null)
      def b = Broadcaster.<String> create()
      b.subscribe(f)

    when: 'a value is pushed downstream'
      b.onNext("1")

    then: 'the value is available as an int'
      f.get() == 1

  }

  def "ListenableFutureProcessor listens for errors"() {

    given:
      def error = null
      def f = new ListenableFutureProcessor()
      f.addCallback(null, { t -> error = t } as FailureCallback)
      def b = Broadcaster.create()
      b.subscribe(f)

    when: 'a value is pushed downstream'
      b.onError(new IllegalArgumentException("test exception"))
      f.get()

    then: 'the error was thrown'
      thrown(IllegalArgumentException)

  }

}
