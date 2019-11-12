package com.oracle.pic.casper.webserver.api.ratelimit;

import com.oracle.pic.casper.mds.loadbalanced.MdsExecutor;
import com.oracle.pic.casper.mds.operator.GrpcEmbargoV3Rule;
import com.oracle.pic.casper.mds.operator.ListEmbargoRuleV1Request;
import com.oracle.pic.casper.mds.operator.ListEmbargoRuleV1Response;
import com.oracle.pic.casper.mds.operator.ListEmbargoV3RulesRequest;
import com.oracle.pic.casper.mds.operator.ListEmbargoV3RulesResponse;
import com.oracle.pic.casper.mds.operator.MdsEmbargoRuleV1;
import com.oracle.pic.casper.mds.operator.OperatorConstants;
import com.oracle.pic.casper.mds.operator.OperatorMetadataServiceGrpc.OperatorMetadataServiceBlockingStub;
import com.oracle.pic.casper.webserver.api.backend.MdsMetrics;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MdsEmbargoRuleCollectorImpl implements EmbargoRuleCollector {

    private final MdsExecutor<OperatorMetadataServiceBlockingStub> client;
    private final Duration requestDeadline;

    public MdsEmbargoRuleCollectorImpl(MdsExecutor<OperatorMetadataServiceBlockingStub> client) {
        this(client, Duration.ofSeconds(30));
    }

    public MdsEmbargoRuleCollectorImpl(MdsExecutor<OperatorMetadataServiceBlockingStub> client,
                                       Duration requestDeadline) {
        this.client = client;
        this.requestDeadline = requestDeadline;
    }

    @Override
    public Collection<MdsEmbargoRuleV1> getEmbargoRules() {
        List<MdsEmbargoRuleV1> embargoRules = new ArrayList<>();
        ListEmbargoRuleV1Response response = null;
        AtomicReference<String> pageToken = new AtomicReference<>("");
        do {
            response = MdsMetrics.executeWithMetrics(MdsMetrics.OPERATOR_MDS_BUNDLE,
                MdsMetrics.OPERATOR_LIST_EMBARGOS_V1,
                false,
                () -> client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                    .listEmbargoRuleV1(ListEmbargoRuleV1Request.newBuilder()
                        .setPageSize(OperatorConstants.MAX_PAGE_SIZE)
                        .setPageToken(pageToken.get())
                        .build())));
            embargoRules.addAll(response.getEmbargoRuleV1List());
            pageToken.set(response.getNextPageToken());
        } while (!response.getNextPageToken().isEmpty());
        return embargoRules;
    }

    @Override
    public Collection<EmbargoV3Rule> getDryRunRules() {
        final List<EmbargoV3Rule> rules = new ArrayList<>();
        final ListEmbargoV3RulesRequest request = ListEmbargoV3RulesRequest.newBuilder().build();
        final ListEmbargoV3RulesResponse response = MdsMetrics.executeWithMetrics(
            MdsMetrics.OPERATOR_MDS_BUNDLE,
            MdsMetrics.OPERATOR_LIST_DRY_RUN_EMBARGOS_V3,
            false,
            () -> client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                .listDryRunEmbargoV3Rules(request)));
        for (GrpcEmbargoV3Rule grpcRule : response.getEmbargoRulesList()) {
            rules.add(EmbargoV3Rule.fromGrpcEmbargoV3Rule(grpcRule));
        }
    return rules;
    }

    @Override
    public Collection<EmbargoV3Rule> getEnabledRules() {
        final List<EmbargoV3Rule> rules = new ArrayList<>();
        String pageToken = "";
        final ListEmbargoV3RulesRequest request = ListEmbargoV3RulesRequest.newBuilder().build();
        final ListEmbargoV3RulesResponse response = MdsMetrics.executeWithMetrics(
            MdsMetrics.OPERATOR_MDS_BUNDLE,
            MdsMetrics.OPERATOR_LIST_ENABLED_EMBARGOS_V3,
            false,
            () -> client.execute(c -> c.withDeadlineAfter(requestDeadline.toMillis(), TimeUnit.MILLISECONDS)
                .listEnabledEmbargoV3Rules(request)));
        for (GrpcEmbargoV3Rule grpcRule : response.getEmbargoRulesList()) {
            rules.add(EmbargoV3Rule.fromGrpcEmbargoV3Rule(grpcRule));
        }
        return rules;
    }
}
