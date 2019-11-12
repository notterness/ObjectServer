package com.oracle.pic.casper.webserver.limit;

import com.google.common.annotations.VisibleForTesting;
import com.oracle.pic.casper.common.config.v2.ResourceLimitConfiguration;
import com.oracle.pic.casper.common.exceptions.InternalServerErrorException;
import com.oracle.pic.casper.common.exceptions.TooBusyException;
import com.oracle.pic.casper.common.resourcecontrol.GlobalResourceControl;
import com.oracle.pic.casper.common.resourcecontrol.ResourceControlTestHook;
import com.oracle.pic.casper.common.resourcecontrol.ResourceTenantException;
import com.oracle.pic.casper.common.resourcecontrol.ResourceUsageRecord;
import com.oracle.pic.casper.common.resourcecontrol.TenantResourceControl;
import com.oracle.pic.casper.webserver.limit.exceptions.ResourceIdentityException;
import org.apache.commons.lang.StringUtils;

public class ResourceLimiter {

    public static final String UNKNOWN_NAMESPACE = "";

    private static final String SERVICE_UNAVAILABLE_MSG = "The service is currently unavailable.";

    private final GlobalResourceControl identityControl;
    private final GlobalResourceControl kmsControl;
    private final GlobalResourceControl limitsControl;
    private final GlobalResourceControl operatorControl;
    private final GlobalResourceControl workRequestControl;
    private final GlobalResourceControl tenantControl;
    private final GlobalResourceControl objectControl;
    private final GlobalResourceControl volumeControl;
    private final TenantResourceControl perTenantControl;

    private final boolean identityTestHook;

    public ResourceLimiter(ResourceLimitConfiguration config) {
        identityTestHook = config.getIdentityTestHook();
        identityControl = new GlobalResourceControl(
                ResourceType.IDENTITY.getName(), config.getIdentityLimit(), identityTestHook);
        kmsControl = new GlobalResourceControl(ResourceType.KMS.getName(), config.getKmsLimit());
        limitsControl = new GlobalResourceControl(ResourceType.LIMITS.getName(), config.getLimitsLimit());
        operatorControl = new GlobalResourceControl(
                ResourceType.OPERATOR_MDS_SERVICE.getName(), config.getOperatorMdsLimit());
        workRequestControl = new GlobalResourceControl(
                ResourceType.WORK_REQUEST_MDS_SERVICE.getName(), config.getWorkRequestMdsLimit());
        tenantControl = new GlobalResourceControl(
                ResourceType.TENANT_MDS_SERVICE.getName(), config.getTenantMdsLimit());
        objectControl = new GlobalResourceControl(
                ResourceType.OBJECT_MDS_SERVICE.getName(), config.getObjectMdsLimit());
        volumeControl = new GlobalResourceControl(
                ResourceType.VOLUME_SERVICE.getName(), config.getVolumeLimit());
        perTenantControl = new TenantResourceControl(config.getPerTenantLimit());
    }

    public ResourceTicket acquireResourceTicket(String namespace, ResourceType resourceType, String context) {
        boolean perTenantResourceAcquired = false;
        boolean globalResourceAcquired = false;
        namespace = StringUtils.lowerCase(namespace);
        ResourceUsageRecord record = new ResourceUsageRecord(namespace + ":" + context);
        try {
            ResourceControlMetrics.RESOURCE_TENANT_REQUEST.inc();
            perTenantControl.acquire(namespace, record);
            ResourceControlMetrics.RESOURCE_TENANT_SUCCESS.inc();
            perTenantResourceAcquired = true;
            acquireGlobalResource(resourceType, record);
            globalResourceAcquired = true;
            return new ResourceTicket(this, resourceType, namespace, record);
        } catch (ResourceTenantException | ResourceIdentityException ex) {
            if (ex instanceof ResourceTenantException) {
                ResourceControlMetrics.RESOURCE_TENANT_FAILURE.inc();
            }
            throw new TooBusyException(SERVICE_UNAVAILABLE_MSG, ex);
        } finally {
            if (!globalResourceAcquired && perTenantResourceAcquired) {
                perTenantControl.release(namespace, record);
            }
        }
    }

    protected void release(String namespace, ResourceType resourceType, ResourceUsageRecord record) {
        namespace = StringUtils.lowerCase(namespace);
        perTenantControl.release(namespace, record);
        releaseGlobalResource(resourceType, record);
    }

    private void acquireWithMetrics(GlobalResourceControl globalResourceControl,
                                    ResourceUsageRecord resourceUsageRecord,
                                    ResourceControlMetricsBundle resourceControlMetricsBundle) {
        try {
            resourceControlMetricsBundle.getRequests().inc();
            globalResourceControl.acquire(resourceUsageRecord);
            resourceControlMetricsBundle.getSuccess().inc();
        } catch (TooBusyException ex) {
            resourceControlMetricsBundle.getFailure().inc();
            final ResourceType resourceType = ResourceType.getEnumValue(globalResourceControl.getResourceName());
            switch (resourceType) {
                case IDENTITY:
                    throw new ResourceIdentityException(ex.getMessage(), ex.getCause());
                default:
                    throw new InternalServerErrorException(
                            String.format("Error invalid resource type %s.", resourceType.getName()));
            }
        }
    }

    private void acquireGlobalResource(ResourceType resourceType, ResourceUsageRecord record) {
        switch (resourceType) {
            case IDENTITY:
                acquireWithMetrics(identityControl, record, ResourceControlMetrics.RESOURCE_IDENTITY);
                break;
            case KMS:
                kmsControl.acquire(record);
                break;
            case LIMITS:
                limitsControl.acquire(record);
                break;
            case OPERATOR_MDS_SERVICE:
                operatorControl.acquire(record);
                break;
            case WORK_REQUEST_MDS_SERVICE:
                workRequestControl.acquire(record);
                break;
            case TENANT_MDS_SERVICE:
                tenantControl.acquire(record);
                break;
            case OBJECT_MDS_SERVICE:
                objectControl.acquire(record);
                break;
            case VOLUME_SERVICE:
                volumeControl.acquire(record);
                break;
            default:
                throw new InternalServerErrorException(
                        String.format("Error invalid resource type %s.", resourceType.getName()));
        }
    }

    private void releaseGlobalResource(ResourceType resourceType, ResourceUsageRecord record) {
        switch (resourceType) {
            case IDENTITY:
                identityControl.release(record);
                ResourceControlMetrics.RESOURCE_IDENTITY.getLatency().update(record.getLatency().toNanos());
                break;
            case KMS:
                kmsControl.release(record);
                break;
            case LIMITS:
                limitsControl.release(record);
                break;
            case OPERATOR_MDS_SERVICE:
                operatorControl.release(record);
                break;
            case WORK_REQUEST_MDS_SERVICE:
                workRequestControl.release(record);
                break;
            case TENANT_MDS_SERVICE:
                tenantControl.release(record);
                break;
            case OBJECT_MDS_SERVICE:
                objectControl.release(record);
                break;
            case VOLUME_SERVICE:
                volumeControl.release(record);
                break;
            default:
                throw new InternalServerErrorException(
                        String.format("Error invalid resource type %s.", resourceType.getName()));
        }
    }

    @VisibleForTesting
    public boolean isEmpty() {
        return identityControl.get() == 0 && kmsControl.get() == 0 && limitsControl.get() == 0 &&
                operatorControl.get() == 0 && workRequestControl.get() == 0 && tenantControl.get() == 0 &&
                objectControl.get() == 0 && volumeControl.get() == 0 && perTenantControl.isEmpty();
    }

    @VisibleForTesting
    public ResourceControlTestHook getIdentityTestHook() {
        return identityControl.getTestHook();
    }
}
