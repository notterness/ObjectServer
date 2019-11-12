package com.oracle.pic.casper.webserver.api.auth;

import com.google.common.base.MoreObjects;
import com.oracle.pic.kms.internal.crypto.sdk.auth.KeyDelegatePermission;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The class represents data for key-delegate permission authorization. It contains the kmsKey and it's permission.
 */
public class KmsKeyUpdateAuth {
    private final String newKmsKey;
    private final String oldKmsKey;
    private final boolean performAssociationAuthz;

    public static final KmsKeyUpdateAuth EMPTY_KMSKEY_UPDATE_AUTH =
            new KmsKeyUpdateAuth(null, null, false);

    public KmsKeyUpdateAuth(String newKmsKey, String oldKmsKey, boolean performAssociationAuthz) {
        this.newKmsKey = newKmsKey;
        this.oldKmsKey = oldKmsKey;
        this.performAssociationAuthz = performAssociationAuthz;
    }

    public boolean isPerformAssociationAuthz() {
        return performAssociationAuthz;
    }

    public String getKmsKeyUpdate() {
        if (newKmsKey == null) {
            return oldKmsKey;
        }

        if (newKmsKey.equals("")) {
            return null;
        }
        return newKmsKey;
    }

    /**
     * In Casper bucket update semantics, 1) if the new value is null means do nothing, user didn't want to touch the
     * current value. 2) if the new value is empty (for String ""), means remove the current value. 3) otherwise update
     * the old value with the new value.
     *
     * For kmsKeyId update we need to determine whether we need to do key-associate/key-disassociate authZ or not.
     * 1) if the new kmsKeyId is null, means do nothing
     * 2) if the new kmsKeyId is "", means remove kmsKeyId from the bucket. If there is already a kmsKeyId associated
     *   with the bucket we need to do key-disassociate authZ, otherwise do nothing
     * 3) if the new kmsKeyId is not null or empty, we need to do update. Need to KEY_ASSOCIATE the newKmsKey
     *   (if newKmsKey is not equal to the oldKmsKey), need to KEY_DISASSOCIATE the oldKmsKey (if oldKmsKey is not null
     *   and it is not equal to newKmsKey)
     */
    public Map<String, KeyDelegatePermission> getKeyDelegatePermissions() {
        final Map<String, KeyDelegatePermission> keyDelegateAuths = new HashMap<>();
        // if newKmsKeyId is null, means user didn't touch the kmsKeyId, no need to do authZ on KeyDelegatePermission
        if (newKmsKey == null) {
            return keyDelegateAuths;
        }
        if (newKmsKey.equals("")) { // remove oldKmsKey, authZ the KEY_DISASSOCIATE on oldKmsKey
            if (oldKmsKey != null) {
                keyDelegateAuths.put(oldKmsKey, KeyDelegatePermission.KEY_DISASSOCIATE);
            }
        } else { // the newKmsKey is not "" or Null
            // If the oldKmsKey is equal to the newKmsKey just do newKmsKey KEY_ASSOCIATE
            // If the oldKmsKey is NOT equal to the newKmsKey, we need to do newKmsKey KEY_ASSOCIATE, and oldKmsKey
            // KEY_DISASSOCIATE check if it is not null.
            keyDelegateAuths.put(newKmsKey, KeyDelegatePermission.KEY_ASSOCIATE);
            if (oldKmsKey != null && !oldKmsKey.equals(newKmsKey)) {
                keyDelegateAuths.put(oldKmsKey, KeyDelegatePermission.KEY_DISASSOCIATE);
            }
        }
        return keyDelegateAuths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof KmsKeyUpdateAuth)) {
            return false;
        }
        KmsKeyUpdateAuth kmsKeyUpdateAuth = (KmsKeyUpdateAuth) o;
        return Objects.equals(newKmsKey, kmsKeyUpdateAuth.newKmsKey) &&
                Objects.equals(oldKmsKey, kmsKeyUpdateAuth.oldKmsKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(newKmsKey, oldKmsKey);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("newKmsKey", newKmsKey)
                .add("oldKmsKey", oldKmsKey)
                .toString();
    }
}
