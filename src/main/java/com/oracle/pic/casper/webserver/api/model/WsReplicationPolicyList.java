package com.oracle.pic.casper.webserver.api.model;

import java.util.List;

public class WsReplicationPolicyList {

    private List<WsReplicationPolicy> replicationPolicyList;
    private String nextStartWith;

    public List<WsReplicationPolicy> getReplicationPolicyList() {
        return replicationPolicyList;
    }

    public void setReplicationPolicyList(List<WsReplicationPolicy> replicationPolicyList) {
        this.replicationPolicyList = replicationPolicyList;
    }

    public String getNextStartWith() {
        return nextStartWith;
    }

    public void setNextStartWith(String nextStartWith) {
        this.nextStartWith = nextStartWith;
    }
}
