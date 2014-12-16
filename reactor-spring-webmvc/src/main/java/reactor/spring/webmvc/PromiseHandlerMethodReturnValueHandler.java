package reactor.spring.webmvc;

import org.springframework.core.MethodParameter;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.context.request.async.DeferredResult;
import org.springframework.web.context.request.async.WebAsyncUtils;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.method.support.ModelAndViewContainer;
import reactor.fn.Consumer;
import reactor.rx.Promise;


/**
 * @author Jon Brisbin
 */
public class PromiseHandlerMethodReturnValueHandler implements HandlerMethodReturnValueHandler {

	@Override
	public boolean supportsReturnType(MethodParameter returnType) {
		return Promise.class.isAssignableFrom(returnType.getParameterType());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleReturnValue(Object returnValue,
	                              final MethodParameter returnType,
	                              final ModelAndViewContainer mavContainer,
	                              final NativeWebRequest webRequest) throws Exception {
		final DeferredResult<Object> deferredResult = new DeferredResult<Object>();
		((Promise)returnValue)
				.onSuccess(new Consumer() {
					@Override
					public void accept(Object o) {
						deferredResult.setResult(o);
					}
				})
				.onError(new Consumer<Throwable>() {
					@Override
					public void accept(Throwable t) {
						deferredResult.setErrorResult(t);
					}
				});

		WebAsyncUtils.getAsyncManager(webRequest)
		             .startDeferredResultProcessing(deferredResult, mavContainer);
	}

}
