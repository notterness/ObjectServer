package com.oracle.pic.casper.webserver.api.model;

import com.oracle.pic.casper.common.encryption.service.DecidingKeyManagementService;
import com.oracle.pic.casper.webserver.api.model.serde.ObjectSummary;

import java.util.List;

public interface ObjectBaseSummary {

    String getObjectName();

    ObjectSummary makeSummary(List<ObjectProperties> properties, DecidingKeyManagementService kms);
}
