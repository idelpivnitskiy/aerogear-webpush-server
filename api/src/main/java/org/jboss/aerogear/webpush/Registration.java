package org.jboss.aerogear.webpush;

import java.net.URI;

/**
 * Represents a client registration in the WebPush protocol.
 */
public interface Registration {

    /**
     * A globally unique identifier for this registration.
     *
     * @return {@code String} the identifier for this registration.
     */
    String id();

    /**
     * The {@link URI} representing this registration.
     * <p>
     *
     * @return {@link URI} which will be returned to the calling client, most often as HTTP Location Header value.
     */
    URI uri();

    /**
     * The {@link URI} used by devices to create new subscriptions
     *
     * @return {@link URI} to be used to create new subscriptions
     */
    URI subscribeUri();

}
