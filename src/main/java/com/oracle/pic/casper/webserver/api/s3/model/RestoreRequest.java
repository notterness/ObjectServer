package com.oracle.pic.casper.webserver.api.s3.model;


import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class RestoreRequest {

    public static final class GlacierJobParameters {
        @JacksonXmlProperty(localName = "Tier")
        private Tier tier;

        public Tier getTier() {
            return tier;
        }

        public void setTier(Tier tier) {
            this.tier = tier;
        }

        public void setTier(String tier) {
            this.tier = Tier.valueOf(tier);
        }

        public GlacierJobParameters() {
            tier = Tier.Standard;
        }

        public GlacierJobParameters(Tier tier) {
            this.tier = tier;
        }

        public GlacierJobParameters(String tier) {
            this.tier = Tier.valueOf(tier);
        }

        public GlacierJobParameters withTier(Tier tier) {
            setTier(tier);
            return this;
        }

        public GlacierJobParameters withTier(String tier) {
            setTier(tier);
            return this;
        }
    }

    @JacksonXmlProperty(localName = "Days")
    private Integer days;

    private GlacierJobParameters glacierJobParameters;

    public Integer getDays() {
        return days;
    }

    public void setDays(Integer days) {
        this.days = days;
    }

    public GlacierJobParameters getGlacierJobParameters() {
        return this.glacierJobParameters;
    }

    public void setGlacierJobParameters(GlacierJobParameters glacierJobParameters) {
        this.glacierJobParameters = glacierJobParameters;
    }

    public RestoreRequest(Integer days) {
        this.days = days;
        this.glacierJobParameters = new GlacierJobParameters();
    }

    public RestoreRequest(GlacierJobParameters glacierJobParameters) {
        this.glacierJobParameters = glacierJobParameters;
    }

    public RestoreRequest(Integer days, GlacierJobParameters glacierJobParameters) {
        this.days = days;
        this.glacierJobParameters = glacierJobParameters;
    }

    public RestoreRequest withGlacierJobParameters(GlacierJobParameters glacierJobParameters) {
        setGlacierJobParameters(glacierJobParameters);
        return this;
    }

    public RestoreRequest withDays(Integer days) {
        setDays(days);
        return this;
    }

    public RestoreRequest() {
        this.days = 0;
        this.glacierJobParameters = new GlacierJobParameters();
    }

}
