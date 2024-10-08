/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.search;

import static org.apache.solr.common.params.CommonParams.NAME;

import java.lang.invoke.MethodHandles;
import java.lang.reflect.Constructor;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.solr.common.ConfigNode;
import org.apache.solr.common.MapSerializable;
import org.apache.solr.common.util.CollectionUtil;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contains the knowledge of how cache config is stored in the solrconfig.xml file, and implements a
 * factory to create caches.
 */
public class CacheConfig implements MapSerializable {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String nodeName;

  /**
   * When this object is created, the core is not yet available . So, if the class is to be loaded
   * from a package we should have a corresponding core
   */
  @SuppressWarnings({"rawtypes"})
  private Supplier<Class<? extends SolrCache>> clazz;

  private Map<String, String> args;
  private CacheRegenerator regenerator;

  private String cacheImpl;

  private Object[] persistence = new Object[1];

  private String regenImpl;

  public CacheConfig() {}

  @SuppressWarnings({"rawtypes"})
  public CacheConfig(
      Class<? extends SolrCache> clazz, Map<String, String> args, CacheRegenerator regenerator) {
    this.clazz = () -> clazz;
    this.cacheImpl = clazz.getName();
    this.args = args;
    this.regenerator = regenerator;
    this.nodeName = args.get(NAME);
  }

  public CacheRegenerator getRegenerator() {
    return regenerator;
  }

  public void setRegenerator(CacheRegenerator regenerator) {
    this.regenerator = regenerator;
  }

  public static Map<String, CacheConfig> getMultipleConfigs(
      SolrResourceLoader loader, SolrConfig solrConfig, String configPath, List<ConfigNode> nodes) {
    if (nodes == null || nodes.isEmpty()) {
      return new LinkedHashMap<>();
    }
    Map<String, CacheConfig> result = CollectionUtil.newHashMap(nodes.size());
    for (ConfigNode node : nodes) {
      if (node.boolAttr("enabled", true)) {
        CacheConfig config =
            getConfig(loader, solrConfig, node.name(), node.attributes().asMap(), configPath);
        result.put(config.args.get(NAME), config);
      }
    }
    return result;
  }

  public static CacheConfig getConfig(SolrConfig solrConfig, ConfigNode node, String xpath) {
    if (!node.boolAttr("enabled", true) || !node.exists()) {
      return null;
    }
    return getConfig(solrConfig, node.name(), node.attributes().asMap(), xpath);
  }

  public static CacheConfig getConfig(
      SolrConfig solrConfig, String nodeName, Map<String, String> attrs, String xpath) {
    return getConfig(solrConfig.getResourceLoader(), solrConfig, nodeName, attrs, xpath);
  }

  public static CacheConfig getConfig(
      SolrResourceLoader loader,
      SolrConfig solrConfig,
      String nodeName,
      Map<String, String> attrs,
      String xpath) {
    CacheConfig config = new CacheConfig();
    config.nodeName = nodeName;
    Map<String, String> attrsCopy = CollectionUtil.newLinkedHashMap(attrs.size());
    for (Map.Entry<String, String> e : attrs.entrySet()) {
      attrsCopy.put(e.getKey(), String.valueOf(e.getValue()));
    }
    attrs = attrsCopy;
    config.args = attrs;

    Map<String, Object> map =
        xpath == null ? null : solrConfig.getOverlay().getEditableSubProperties(xpath);
    if (map != null) {
      HashMap<String, String> mapCopy = new HashMap<>(config.args);
      for (Map.Entry<String, Object> e : map.entrySet()) {
        mapCopy.put(e.getKey(), String.valueOf(e.getValue()));
      }
      config.args = mapCopy;
    }
    String nameAttr = config.args.get(NAME); // OPTIONAL
    if (nameAttr == null) {
      config.args.put(NAME, config.nodeName);
    }

    config.cacheImpl = config.args.get("class");
    if (config.cacheImpl == null) config.cacheImpl = "solr.CaffeineCache";
    config.clazz =
        new Supplier<>() {
          @SuppressWarnings("rawtypes")
          Class<? extends SolrCache> loadedClass;

          @Override
          @SuppressWarnings("rawtypes")
          public Class<? extends SolrCache> get() {
            if (loadedClass != null) return loadedClass;
            return loadedClass =
                loader.findClass(
                    new PluginInfo("cache", Collections.singletonMap("class", config.cacheImpl)),
                    SolrCache.class,
                    true);
          }
        };
    config.regenImpl = config.args.get("regenerator");
    if (config.regenImpl != null) {
      config.regenerator = loader.newInstance(config.regenImpl, CacheRegenerator.class);
    }

    return config;
  }

  @SuppressWarnings("rawtypes")
  public SolrCache newInstance(SolrCore core) {
    try {
      @SuppressWarnings("unchecked")
      SolrCache<?, ?> cache = newInstance(core, (Class<? extends SolrCache<?, ?>>) clazz.get());
      persistence[0] = cache.init(args, persistence[0], regenerator);
      return cache;
    } catch (Exception e) {
      log.error("Error instantiating cache", e);
      // we can carry on without a cache... but should we?
      // in some cases (like an OOM) we probably should try to continue.
      return null;
    }
  }

  private SolrCache<?, ?> newInstance(SolrCore core, Class<? extends SolrCache<?, ?>> clazz)
      throws Exception {
    // TODO: pass `SolrCore` as an init arg instead of as a ctor arg; see:
    //  https://issues.apache.org/jira/browse/SOLR-16654
    //  https://github.com/apache/solr/pull/1351
    for (Constructor<?> con : clazz.getConstructors()) {
      Class<?>[] types = con.getParameterTypes();
      if (types.length == 1 && types[0] == SolrCore.class) {
        return (SolrCache<?, ?>) con.newInstance(core);
      }
    }
    return clazz.getConstructor().newInstance();
  }

  @Override
  public Map<String, Object> toMap(Map<String, Object> argsMap) {
    // TODO: Should not create new HashMap?
    return new HashMap<>(args);
  }

  public String getNodeName() {
    return nodeName;
  }
}
