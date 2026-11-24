package com.miet.tpp.messagebroker.server;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Реестр занятых clientId среди подключённых клиентов.
 */
public final class ClientRegistry {

    private final Set<String> ids = ConcurrentHashMap.newKeySet();

    public boolean tryRegister(String clientId) {
        return ids.add(clientId);
    }

    public void unregister(String clientId) {
        if (clientId != null) {
            ids.remove(clientId);
        }
    }
}
