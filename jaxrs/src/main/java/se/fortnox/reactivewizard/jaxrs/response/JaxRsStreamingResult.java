package se.fortnox.reactivewizard.jaxrs.response;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.netty.http.server.HttpServerResponse;
import rx.functions.Func1;

import java.util.Map;

public class JaxRsStreamingResult<T> extends JaxRsResult<T> {
    public JaxRsStreamingResult(Flux<T> output, HttpResponseStatus responseStatus, Func1<T, byte[]> serializer, Map<String, String> headers) {
        super(output, responseStatus, serializer, headers);
    }

    @Override
    public Publisher<Void> write(HttpServerResponse response) {
        // switchOnFirst will delay sending headers until the first part of the output stream arrives
        return output.switchOnFirst((firstValue, innerFlux) -> {
            // From the docs:
            // Note that the source might complete or error immediately instead of emitting, in which case the Signal would be onComplete or onError.
            if (firstValue.isOnError()) {
                // Dont send response status and headers here, it will be sent by the upstream exceptionhandler
                return innerFlux.cast(Void.class);
            }
            response.status(responseStatus);
            headers.forEach(response::addHeader);
            return response.sendByteArray(innerFlux.map(serializer::call));
        });
    }
}
