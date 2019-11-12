package com.oracle.pic.casper.webserver.api.common;

import com.oracle.pic.tagging.client.entities.TaggingClient;
import com.oracle.pic.tagging.client.entities.TaggingClientImpl;
import com.oracle.pic.tagging.client.tag.TagSet;
import com.oracle.pic.tagging.common.exception.TagSetCreationException;
import com.oracle.pic.tagging.common.exception.TagSlugCreationException;

public final class TaggingClientHelper {
    private static final TaggingClient TAGGING_CLIENT = new TaggingClientImpl();

    private TaggingClientHelper() {

    }

    public static TagSet extractTagSet(byte[] tagSlug) throws TagSetCreationException {
        return TAGGING_CLIENT.extractTagSet(tagSlug);
    }

    public static byte[] toByteArray(TagSet tagSet) throws TagSlugCreationException {
        return TAGGING_CLIENT.toByteArray(tagSet);
    }
}
