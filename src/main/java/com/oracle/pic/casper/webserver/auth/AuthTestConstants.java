package com.oracle.pic.casper.webserver.auth;

import com.google.common.collect.ImmutableMap;
import com.oracle.pic.commons.metadata.entities.Tenant;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public final class AuthTestConstants {

    public static final String FAKE_TENANT_ID = "ocid1.tenancy.oc1.." +
            "aaaaaaaaaodntvb6nij46dccx2dqn6a3xs563vhqm7ay5bkn4wbqvb2a3bya";
    public static final String FAKE_SUBJECT_ID = "fake.subject.id";
    public static final String FAKE_COMPARTMENT_ID = "ocid1.compartment.oc1.." +
            "aaaaaaaatpq4vofrmhsosnebgk5j2xtksohnko7uqm4kd4aezbueennzqmaq";
    public static final String FAKE_COMPARTMENT_A_ID = "ocid1.compartment.oc1.." +
            "aaaaaaaatpq4vofrmhsosnebgk5j6xtksohnko7uqm4kd4aezbueennzqmaq";
    public static final String FAKE_COMPARTMENT_B_ID = "ocid1.compartment.oc1.." +
            "aaaaaaaatpq5vofrmhsosnebgk5j2xtksohnko7uqm4kd4aezbueennzqmaq";
    public static final String FAKE_TENANT_NAME = "faketenantname";
    public static final String FAKE_COMPARTMENT_NAME = "fakecompartmentname";
    public static final String FAKE_COMPARTMENT_DISPLAY_NAME = "fakecompartmentdisplayname";
    public static final String FAKE_COMPARTMENT_FULL_NAME = "fakecompartmentfullname";
    public static final String FAKE_NAMESPACE_NAME = "faketenantname";
    public static final String FAKE_HOME_REGION = "fakehomeregion";

    public static final String CROSS_TENANT_ID = "ocid1.tenancy.oc1.." +
        "aaaaaaaaqcefd5x3dfedzczw7l4eqmfv5g3d4qeaus6ltkdbspfg2qtg3d6q";
    public static final String CROSS_TENANT_SUBJECT_ID = "cross.subject.id";
    public static final String CROSS_TENANT_COMPARTMENT_ID = "ocid1.compartment.oc1.." +
        "aaaaaaaatpq5vofrmhsosnebgk5j2xtksohnko7uqm5kd4aezbueennzqmaq";
    public static final String CROSS_TENANT_TENANT_NAME = "crosstenantname";
    public static final String CROSS_TENANT_NAMESPACE_NAME = "ah32417j";

    public static final Map<Tenant, List<String>> TENANTS_AND_COMPARTMENTS = ImmutableMap.of(
        new Tenant(FAKE_TENANT_ID, FAKE_TENANT_NAME, FAKE_NAMESPACE_NAME, FAKE_HOME_REGION),
        Arrays.asList(FAKE_COMPARTMENT_ID, "__fake_v1_compartment_id__",
                FAKE_COMPARTMENT_A_ID, FAKE_COMPARTMENT_B_ID),
        new Tenant(CROSS_TENANT_ID, CROSS_TENANT_TENANT_NAME, CROSS_TENANT_NAMESPACE_NAME, FAKE_HOME_REGION),
        Arrays.asList(CROSS_TENANT_COMPARTMENT_ID));

    private AuthTestConstants() {

    }
}
