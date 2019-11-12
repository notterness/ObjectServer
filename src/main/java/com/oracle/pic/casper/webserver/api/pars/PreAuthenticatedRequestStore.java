package com.oracle.pic.casper.webserver.api.pars;

import com.oracle.pic.casper.webserver.api.model.CreatePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.DeletePreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.GetPreAuthenticatedRequestRequest;
import com.oracle.pic.casper.webserver.api.model.ListPreAuthenticatedRequestsRequest;
import com.oracle.pic.casper.webserver.api.model.PreAuthenticatedRequestMetadata;
import com.oracle.pic.casper.webserver.api.model.ParPaginatedList;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Interface for store to manage pre authenticated requests (PARs)
 */
public interface PreAuthenticatedRequestStore {

    /**
     * Create / provision a pre-authenticated request in the store
     * @param createParRequest - the pre authenticated request
     */
    CompletableFuture<PreAuthenticatedRequestMetadata> createPreAuthenticatedRequest(
            CreatePreAuthenticatedRequestRequest createParRequest);

    /**
     * Fetch pre authenticated request from store.
     * @param getParRequest the get PAR request
     * @return the pre authenticated request point to by the specifiec parId
     */
    CompletableFuture<PreAuthenticatedRequestMetadata> getPreAuthenticatedRequest(
            GetPreAuthenticatedRequestRequest getParRequest);

    /**
     * Delete the pre authenticated request from the store.
     * @param deleteParRequest the delete PAR request
     */
    void deletePreAuthenticatedRequest(
            DeletePreAuthenticatedRequestRequest deleteParRequest);

    /**
     * List pre authenticated requests for the specified bucket
     * @param listParRequest the list PAR request
     * @return list of PARs that match the specified criteria
     */
    CompletableFuture<ParPaginatedList<PreAuthenticatedRequestMetadata>> listPreAuthenticatedRequests(
            ListPreAuthenticatedRequestsRequest listParRequest);

    /**
     * Update / extend expiration for the pre authenticated request
     * @param parId unique identifier to pre authenticated request
     * @param expiration new future expiration date to update PAR with
     */
    CompletableFuture<Void> updateExpiration(String parId, Instant expiration);

    /**
     * Update status for pre authenticated request
     * @param parId unique identifier to pre authenticated request
     * @param status PAR status to update e.g  ACTIVE / REVOKED etc
     */
    CompletableFuture<Void> updateStatus(String parId, PreAuthenticatedRequestMetadata.Status status);
}
