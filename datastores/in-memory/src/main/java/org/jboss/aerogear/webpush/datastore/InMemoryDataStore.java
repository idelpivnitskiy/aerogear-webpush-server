/**
 * JBoss, Home of Professional Open Source
 * Copyright Red Hat, Inc., and individual contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jboss.aerogear.webpush.datastore;


import org.jboss.aerogear.webpush.NewSubscription;
import org.jboss.aerogear.webpush.PushMessage;
import org.jboss.aerogear.webpush.Registration;
import org.jboss.aerogear.webpush.Subscription;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link DataStore} implementation that stores all information in memory.
 */
public class InMemoryDataStore implements DataStore {

    public static final byte[] EMPTY_BYTES = {};
    private final ConcurrentMap<String, Registration> registrations = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<Subscription>> subscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, NewSubscription> newSubscriptions = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<PushMessage>> waitingDeliveryMessages = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ConcurrentMap<String, PushMessage>> sentMessages = new ConcurrentHashMap<>();

    private byte[] salt;

    @Override
    public void saveNewSubscription(NewSubscription subscription) {
        Objects.requireNonNull(subscription, "subscription must not be null");
        newSubscriptions.putIfAbsent(subscription.id(), subscription);
    }

    @Override
    public Optional<NewSubscription> getNewSubscription(String id) {
        return Optional.ofNullable(newSubscriptions.get(id));
    }

    @Override
    public List<PushMessage> removeNewSubscription(String id) {
        List<PushMessage> result = null;
        NewSubscription subscription;
        List<PushMessage> waitingDelivery;
        ConcurrentMap<String, PushMessage> sent;
        do {
            subscription = newSubscriptions.remove(id);
            waitingDelivery = waitingDeliveryMessages.remove(id);
            sent = sentMessages.remove(id);
            if (sent != null) {
                if (result == null) {
                    result = new ArrayList<>(sent.values());
                } else {
                    result.addAll(sent.values());
                }
            }
        } while (subscription != null || waitingDelivery != null || sent != null);
        return result != null ? result : Collections.emptyList();
    }

    @Override
    public void saveMessage(PushMessage msg) {
        Objects.requireNonNull(msg, "push message can not be null");
        String subId = msg.subscription();
        List<PushMessage> currentList = waitingDeliveryMessages.get(subId);
        if (currentList != null) {
            currentList.add(msg);
        } else {
            List<PushMessage> newList = Collections.synchronizedList(new ArrayList<>());
            newList.add(msg);
            List<PushMessage> previousList = waitingDeliveryMessages.putIfAbsent(subId, newList);
            if (previousList != null) {
                previousList.add(msg);
            }
        }
    }

    @Override
    public List<PushMessage> waitingDeliveryMessages(String subId) {
        List<PushMessage> currentList = waitingDeliveryMessages.remove(subId);
        if (currentList == null) {
            return Collections.emptyList();
        }
        return Collections.unmodifiableList(currentList);
    }

    @Override
    public void saveSentMessage(PushMessage msg) {
        Objects.requireNonNull(msg, "push message can not be null");
        if (!msg.receiptSubscription().isPresent()) {
            throw new IllegalArgumentException("push message must have receipt subscription URI");
        }

        String subId = msg.subscription();
        ConcurrentMap<String, PushMessage> currentMap = sentMessages.get(subId);
        if (currentMap == null) {
            ConcurrentMap<String, PushMessage> newMap = new ConcurrentHashMap<>();
            ConcurrentMap<String, PushMessage> previousMap = sentMessages.putIfAbsent(subId, newMap);
            currentMap = previousMap != null ? previousMap : newMap;
        }
        currentMap.put(msg.id(), msg);
    }

    @Override
    public Optional<PushMessage> sentMessage(String subId, String msgId) {
        ConcurrentMap<String, PushMessage> currentMap = sentMessages.get(subId);
        if (currentMap != null) {
            return Optional.ofNullable(currentMap.remove(msgId));
        }
        return Optional.empty();
    }

    @Override
    public void savePrivateKeySalt(final byte[] salt) {
        if (this.salt != null) {
            this.salt = salt;
        }
    }

    @Override
    public byte[] getPrivateKeySalt() {
        if (salt == null) {
            return EMPTY_BYTES;
        }
        return salt;
    }

    @Override
    public boolean saveRegistration(final Registration registration) {
        Objects.requireNonNull(registration, "registration must not be null");
        return registrations.putIfAbsent(registration.id(), registration) == null;
    }

    @Override
    public Optional<Registration> getRegistration(final String id) {
        return Optional.ofNullable(registrations.get(id));
    }

    @Override
    public void saveChannel(final Subscription subscription) {
        final String id = subscription.registrationId();
        final Set<Subscription> newSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
        newSubscriptions.add(subscription);
        while (true) {
            final Set<Subscription> currentSubscriptions = subscriptions.get(id);
            if (currentSubscriptions == null) {
                final Set<Subscription> previous = subscriptions.putIfAbsent(id, newSubscriptions);
                if (previous != null) {
                    newSubscriptions.addAll(previous);
                    if (subscriptions.replace(id, previous, newSubscriptions)) {
                        break;
                    }
                } else {
                    break;
                }
            } else {
                newSubscriptions.addAll(currentSubscriptions);
                if (subscriptions.replace(id, currentSubscriptions, newSubscriptions)) {
                    break;
                }
            }
        }
    }

    @Override
    public void removeChannel(final Subscription subscription) {
        Objects.requireNonNull(subscription, "subscription must not be null");
        while (true) {
            final Set<Subscription> currentSubscriptions = subscriptions.get(subscription.registrationId());
            if (currentSubscriptions == null || currentSubscriptions.isEmpty()) {
                break;
            }
            final Set<Subscription> newSubscriptions = Collections.newSetFromMap(new ConcurrentHashMap<>());
            boolean added = newSubscriptions.addAll(currentSubscriptions);
            if (!added){
                break;
            }

            boolean removed = newSubscriptions.remove(subscription);
            if (removed) {
                if (subscriptions.replace(subscription.registrationId(), currentSubscriptions, newSubscriptions)) {
                    break;
                }
            }
        }
    }

    @Override
    public Set<Subscription> getSubscriptions(final String registrationId) {
        final Set<Subscription> subscriptions = this.subscriptions.get(registrationId);
        if (subscriptions == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(subscriptions);
    }

}
