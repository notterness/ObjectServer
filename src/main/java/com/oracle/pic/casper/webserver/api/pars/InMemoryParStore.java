package com.oracle.pic.casper.webserver.api.pars;

import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.DeletePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.GetPreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.ListPreAuthenticatedRequestsRequest;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.ParPaginatedList;
import com.oracle.pic.casper.webserver.api.model.exceptions.ParAlreadyExistsException;
import com.oracle.pic.casper.webserver.api.model.exceptions.ParNotFoundException;
import org.apache.commons.lang.NotImplementedException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class InMemoryParStore implements PreAuthenticatedRequestStore {

    private final ConcurrentMap<String, PreAuthenticatedRequestMetadata> store = new ConcurrentHashMap<>();

    @Override
    public CompletableFuture<PreAuthenticatedRequestMetadata> createPreAuthenticatedRequest(
            CreatePreAuthenticatedRequestRequest request) {

        String parId = request.getVerifierId();
        if (request.getObjectName().isPresent()) {
            parId = request.getObjectName().get() + "#" + request.getVerifierId();
         }
        PreAuthenticatedRequestMetadata parMd = new PreAuthenticatedRequestMetadata(
                parId,
                request.getName(),
                request.getAccessType(),
                request.getBucketName(),
                request.getObjectName().orElse(null),
                PreAuthenticatedRequestMetadata.Version.V1,
                PreAuthenticatedRequestMetadata.Status.Active,
                request.getAuthInfo(),
                Instant.now(),
                request.getTimeExpires());

        PreAuthenticatedRequestMetadata oldPar = store.putIfAbsent(parMd.getParId(), parMd);
        if (oldPar != null) {
            CompletableFuture<PreAuthenticatedRequestMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new ParAlreadyExistsException());
            return future;
        }

        return CompletableFuture.completedFuture(parMd);
    }

    @Override
    public CompletableFuture<PreAuthenticatedRequestMetadata> getPreAuthenticatedRequest(
            GetPreAuthenticatedRequestRequest request) {
        PreAuthenticatedRequestMetadata par = store.get(request.getParId().toString());
        if (par == null) {
            CompletableFuture<PreAuthenticatedRequestMetadata> future = new CompletableFuture<>();
            future.completeExceptionally(new ParNotFoundException());
            return future;
        }
        return CompletableFuture.completedFuture(par);
    }

    @Override
    public void deletePreAuthenticatedRequest(DeletePreAuthenticatedRequestRequest request) {
        PreAuthenticatedRequestMetadata par = store.get(request.getParId().toString());
        if (par == null) {
            throw new ParNotFoundException();
        }
        store.remove(par.getParId());
    }

    /**
     * In-memory version does not support filtering by parUrl.
     */
    @Override
    public CompletableFuture<ParPaginatedList<PreAuthenticatedRequestMetadata>> listPreAuthenticatedRequests(
            ListPreAuthenticatedRequestsRequest request) {

        String bucketName = request.getBucketName();
        Optional<String> objectNamePrefix = request.getObjectNamePrefix();
        int pageLimit = request.getPageLimit();
        // get the canonical list of PARs from the store.
        // filter out any PARs that match the object name prefix if present
        List<String> parIds = store.values().stream()
                .filter(parMd -> parMd.getBucketName().equals(bucketName))
                .filter(parMd -> {
                    if (!objectNamePrefix.isPresent()) {
                        return true; // match everything
                    } else {
                        return  parMd.getObjectName().isPresent() &&
                                parMd.getObjectName().get().startsWith(objectNamePrefix.get());
                    }
                })
                .map(PreAuthenticatedRequestMetadata::getParId)
                .collect(Collectors.toList());

        if (parIds.size() == 0) {
            return CompletableFuture.completedFuture(new ParPaginatedList<>(new ArrayList<>(), null));
        }

        // sort all parIds
        Collections.sort(parIds);

        // note that startOffset will be inclusive and endOffset will be exclusive
        // first find the startOffset
        int startOffset = 0;    // the default value in case nextPageToken is null
        if (request.getNextPageToken().isPresent()) {
            startOffset = parIds.indexOf(request.getNextPageToken().get());
            if (startOffset == -1) {
                // token does not match anything in our list and hence this is not a valid input
                return CompletableFuture.completedFuture(new ParPaginatedList<>(new ArrayList<>(), null));
            }
        }
        // note that endOffset is exclusive
        int endOffset = startOffset + pageLimit;

        List<PreAuthenticatedRequestMetadata> result = new ArrayList<>();
        int index = startOffset;
        while (index < parIds.size() && index < endOffset) {
            String parId = parIds.get(index++);
            result.add(store.get(parId));
        }

        String nextPageToken = (index == parIds.size()) ? null : parIds.get(index);
        return CompletableFuture.completedFuture(new ParPaginatedList<>(result, nextPageToken));
    }

    @Override
    public CompletableFuture<Void> updateExpiration(String parId, Instant expiration) {
        throw new NotImplementedException("not yet implemented");
    }

    @Override
    public CompletableFuture<Void> updateStatus(String parId, PreAuthenticatedRequestMetadata.Status status) {
        throw new NotImplementedException("not yet implemented");
    }
}
