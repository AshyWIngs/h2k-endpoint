package kz.qazmarka.h2k.payload.serializer.avro;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

final class SchemaRegistryErrors {
    static final int HTTP_BAD_REQUEST = 400;
    static final int HTTP_UNAUTHORIZED = 401;
    static final int HTTP_FORBIDDEN = 403;
    static final int HTTP_CONFLICT = 409;
    static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    static final int HTTP_REQUEST_TIMEOUT = 408;
    static final int HTTP_TOO_MANY_REQUESTS = 429;
    static final int HTTP_INTERNAL_ERROR = 500;
    static final int HTTP_BAD_GATEWAY = 502;
    static final int HTTP_SERVICE_UNAVAILABLE = 503;
    static final int HTTP_GATEWAY_TIMEOUT = 504;

    private static final Set<Integer> RETRYABLE_STATUSES = buildRetryableStatuses();

    private SchemaRegistryErrors() {}

    static String summary(RestClientException ex) {
        if (ex == null) {
            return "ошибка=неизвестно";
        }
        return "статус=" + ex.getStatus() + ", код=" + ex.getErrorCode() + ", сообщение=" + ex.getMessage();
    }

    static boolean isRetryable(RestClientException ex) {
        return ex != null && (ex.getStatus() <= 0 || RETRYABLE_STATUSES.contains(ex.getStatus()));
    }

    static boolean isAuthError(RestClientException ex) {
        int status = ex == null ? Integer.MIN_VALUE : ex.getStatus();
        return status == HTTP_UNAUTHORIZED || status == HTTP_FORBIDDEN;
    }

    static boolean isIncompatible(RestClientException ex) {
        int status = ex == null ? Integer.MIN_VALUE : ex.getStatus();
        return status == HTTP_CONFLICT;
    }

    static boolean isInvalidSchema(RestClientException ex) {
        int status = ex == null ? Integer.MIN_VALUE : ex.getStatus();
        return status == HTTP_BAD_REQUEST || status == HTTP_UNPROCESSABLE_ENTITY;
    }

    private static Set<Integer> buildRetryableStatuses() {
        Set<Integer> statuses = new HashSet<>(8);
        statuses.add(HTTP_REQUEST_TIMEOUT);
        statuses.add(HTTP_TOO_MANY_REQUESTS);
        statuses.add(HTTP_INTERNAL_ERROR);
        statuses.add(HTTP_BAD_GATEWAY);
        statuses.add(HTTP_SERVICE_UNAVAILABLE);
        statuses.add(HTTP_GATEWAY_TIMEOUT);
        return Collections.unmodifiableSet(statuses);
    }
}
