package org.apache.solr.util.circuitbreaker;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.annotation.JsonProperty;
import org.apache.solr.common.cloud.ClusterPropertiesListener;
import org.apache.solr.common.util.Utils;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.util.SolrJacksonAnnotationInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobalCircuitBreakerManager implements ClusterPropertiesListener {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final ObjectMapper mapper = SolrJacksonAnnotationInspector.createObjectMapper();
  private final GlobalCircuitBreakerFactory factory;
  private final CoreContainer coreContainer;
  private GlobalCircuitBreakerConfig currentConfig;

  private static volatile GlobalCircuitBreakerManager instance;

  public static void init(CoreContainer coreContainer) {
    if (instance == null) {
      synchronized (GlobalCircuitBreakerManager.class) {
        instance = new GlobalCircuitBreakerManager(coreContainer);
        coreContainer
            .getZkController()
            .getZkStateReader()
            .registerClusterPropertiesListener(instance);
      }
    }
  }

  public GlobalCircuitBreakerManager(CoreContainer coreContainer) {
    super();
    this.factory = new GlobalCircuitBreakerFactory(coreContainer);
    this.coreContainer = coreContainer;
  }

  private static class GlobalCircuitBreakerConfig {
    static final String CIRCUIT_BREAKER_CLUSTER_PROPS_KEY = "circuit-breakers";
    @JsonProperty Map<String, CircuitBreakerConfig> configs = new ConcurrentHashMap<>();

    @JsonProperty
    Map<String, Map<String, CircuitBreakerConfig>> hostOverrides = new ConcurrentHashMap<>();

    static class CircuitBreakerConfig {
      @JsonProperty Boolean enabled = false;
      @JsonProperty Boolean warnOnly = false;
      @JsonProperty Double updateThreshold = Double.MAX_VALUE;
      @JsonProperty Double queryThreshold = Double.MAX_VALUE;

      @Override
      public int hashCode() {
        return Objects.hash(enabled, warnOnly, updateThreshold, queryThreshold);
      }

      @Override
      public boolean equals(Object obj) {
        if (obj instanceof CircuitBreakerConfig) {
          CircuitBreakerConfig that = (CircuitBreakerConfig) obj;
          return that.enabled.equals(this.enabled)
              && that.warnOnly.equals(this.warnOnly)
              && that.updateThreshold.equals(this.updateThreshold)
              && that.queryThreshold.equals(this.queryThreshold);
        }
        return false;
      }
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof GlobalCircuitBreakerConfig) {
        GlobalCircuitBreakerConfig that = (GlobalCircuitBreakerConfig) o;
        return that.configs.equals(this.configs) && that.hostOverrides.equals(this.hostOverrides);
      }
      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(configs, hostOverrides);
    }
  }

  // for registering global circuit breakers set in clusterprops
  @Override
  public boolean onChange(Map<String, Object> properties) {
    try {
      GlobalCircuitBreakerConfig nextConfig = processConfigChange(properties);
      if (nextConfig != null && !nextConfig.equals(this.currentConfig)) {
        this.currentConfig = nextConfig;
        registerCircuitBreakers(nextConfig);
      }
    } catch (Exception e) {
      if (log.isWarnEnabled()) {
        // don't break when things are misconfigured
        log.warn("error parsing global circuit breaker configuration {}", e);
      }
    }
    return false;
  }

  private GlobalCircuitBreakerConfig processConfigChange(Map<String, Object> properties)
      throws IOException {
    Object cbConfig = properties.get(GlobalCircuitBreakerConfig.CIRCUIT_BREAKER_CLUSTER_PROPS_KEY);
    GlobalCircuitBreakerConfig globalCBConfig = null;
    if (cbConfig != null) {
      byte[] configInput = Utils.toJSON(cbConfig);
      if (configInput != null && configInput.length > 0) {
        globalCBConfig = mapper.readValue(configInput, GlobalCircuitBreakerConfig.class);
      }
    }
    return globalCBConfig;
  }

  private void registerCircuitBreakers(GlobalCircuitBreakerConfig gbConfig) throws Exception {
    CircuitBreakerRegistry.deregisterGlobal();
    for (Map.Entry<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> entry :
        gbConfig.configs.entrySet()) {
      GlobalCircuitBreakerConfig.CircuitBreakerConfig config =
          getConfig(gbConfig, entry.getKey(), entry.getValue());
      try {
        if (config.enabled) {
          if (config.queryThreshold != Double.MAX_VALUE) {
            registerGlobalCircuitBreaker(
                this.factory.create(entry.getKey()),
                config.queryThreshold,
                SolrRequest.SolrRequestType.QUERY,
                config.warnOnly);
          }
          if (config.updateThreshold != Double.MAX_VALUE) {
            registerGlobalCircuitBreaker(
                this.factory.create(entry.getKey()),
                config.updateThreshold,
                SolrRequest.SolrRequestType.UPDATE,
                config.warnOnly);
          }
        }
      } catch (Exception e) {
        if (log.isWarnEnabled()) {
          log.warn("error while registering global circuit breaker {}: {}", entry.getKey(), e);
        }
      }
    }
  }

  private GlobalCircuitBreakerConfig.CircuitBreakerConfig getConfig(
      GlobalCircuitBreakerConfig gbConfig,
      String className,
      GlobalCircuitBreakerConfig.CircuitBreakerConfig globalConfig) {
    Map<String, GlobalCircuitBreakerConfig.CircuitBreakerConfig> thisHostOverrides =
        gbConfig.hostOverrides.get(getHostName());
    if (thisHostOverrides != null && thisHostOverrides.get(className) != null) {
      if (log.isInfoEnabled()) {
        log.info("overriding circuit breaker {} for host {}", className, getHostName());
      }
      return thisHostOverrides.get(className);
    }
    return globalConfig;
  }

  private String getHostName() {
    return this.coreContainer.getHostName() != null
        ? this.coreContainer.getHostName()
        : "localhost";
  }

  private void registerGlobalCircuitBreaker(
      CircuitBreaker globalCb,
      double threshold,
      SolrRequest.SolrRequestType type,
      boolean warnOnly) {
    globalCb.setThreshold(threshold);
    globalCb.setRequestTypes(List.of(type.name()));
    globalCb.setWarnOnly(warnOnly);
    CircuitBreakerRegistry.registerGlobal(globalCb);
    if (log.isInfoEnabled()) {
      log.info("onChange registered circuit breaker {}", globalCb);
    }
  }
}