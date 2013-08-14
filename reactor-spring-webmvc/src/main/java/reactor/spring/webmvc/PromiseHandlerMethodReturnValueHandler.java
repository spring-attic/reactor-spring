package reactor.spring.webmvc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.MethodParameter;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.method.support.HandlerMethodReturnValueHandler;
import org.springframework.web.method.support.ModelAndViewContainer;
import reactor.core.composable.Promise;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.util.List;

/**
 * @author Jon Brisbin
 */
public class PromiseHandlerMethodReturnValueHandler implements HandlerMethodReturnValueHandler {

	private static final Logger LOG = LoggerFactory.getLogger(PromiseHandlerMethodReturnValueHandler.class);

	private final List<HandlerMethodReturnValueHandler> delegates;

	public PromiseHandlerMethodReturnValueHandler(List<HandlerMethodReturnValueHandler> delegates) {
		Assert.notNull(delegates, "Delegate HandlerMethodReturnValueHandler cannot be null.");
		this.delegates = delegates;
	}

	@Override
	public boolean supportsReturnType(MethodParameter returnType) {
		if (!Promise.class.isAssignableFrom(returnType.getParameterType())) {
			return false;
		}

		MethodParameter promiseParam = resolvePromiseType(returnType);
		for (HandlerMethodReturnValueHandler delegate : delegates) {
			if (delegate instanceof PromiseHandlerMethodReturnValueHandler) {
				continue;
			}
			if (delegate.supportsReturnType(promiseParam)) {
				return true;
			}
		}

		return false;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void handleReturnValue(Object returnValue,
																final MethodParameter returnType,
																final ModelAndViewContainer mavContainer,
																final NativeWebRequest webRequest) throws Exception {
		Object obj = ((Promise) returnValue).await();

		MethodParameter promiseParam = resolvePromiseType(returnType);
		for (HandlerMethodReturnValueHandler delegate : delegates) {
			if (delegate instanceof PromiseHandlerMethodReturnValueHandler) {
				continue;
			}
			if (delegate.supportsReturnType(promiseParam)) {
				delegate.handleReturnValue(obj, promiseParam, mavContainer, webRequest);
				return;
			}
		}
	}

	private MethodParameter resolvePromiseType(MethodParameter mp) {
		Class<?> promiseType = (Class<?>) ((ParameterizedType) ((ParameterizedType) mp.getGenericParameterType()).getActualTypeArguments()[0]).getRawType();

		MethodParameter promiseParam = new MethodParameter(mp.getMethod(), -1);
		Field fld = ReflectionUtils.findField(MethodParameter.class, "parameterType");
		ReflectionUtils.makeAccessible(fld);
		try {
			fld.set(promiseParam, promiseType);
		} catch (IllegalAccessException e) {
			throw new IllegalStateException(e);
		}

		return promiseParam;
	}

}
