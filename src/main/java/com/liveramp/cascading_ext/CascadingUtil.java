/**
 *  Copyright 2012 LiveRamp
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.liveramp.cascading_ext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStepStrategy;
import cascading.flow.hadoop.HadoopFlowProcess;

import com.liveramp.cascading_ext.bloom.BloomAssemblyStrategy;
import com.liveramp.cascading_ext.bloom.BloomProps;
import com.liveramp.cascading_ext.flow.JobPersister;
import com.liveramp.cascading_ext.flow.LoggingFlowConnector;
import com.liveramp.cascading_ext.flow_step_strategy.FlowStepStrategyFactory;
import com.liveramp.cascading_ext.flow_step_strategy.MultiFlowStepStrategy;
import com.liveramp.cascading_ext.flow_step_strategy.RenameJobStrategy;
import com.liveramp.cascading_ext.flow_step_strategy.SimpleFlowStepStrategyFactory;
import com.liveramp.cascading_ext.util.OperationStatsUtils;

public class CascadingUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CascadingUtil.class);

  public static final String CASCADING_RUN_ID = "cascading_ext.cascading.run.id";

  private static final CascadingUtil INSTANCE = new CascadingUtil();

  public static CascadingUtil get() {
    return INSTANCE;
  }

  protected CascadingUtil() {
    defaultFlowStepStrategies.addAll(getDefaultFlowStepStrategies());
    defaultProperties.putAll(BloomProps.getDefaultProperties());
  }

  private final Map<Object, Object> defaultProperties = new HashMap<Object, Object>();
  private final List<FlowStepStrategyFactory<JobConf>> defaultFlowStepStrategies = new ArrayList<FlowStepStrategyFactory<JobConf>>();
  private final Set<Class<? extends Serialization>> serializations = new HashSet<Class<? extends Serialization>>();
  private final Map<Integer, Class<?>> serializationTokens = new HashMap<Integer, Class<?>>();
  private final Multimap<String, String> invalidPropertyValues = HashMultimap.create();

  private transient JobConf conf = null;

  public void setDefaultProperty(Object key, Object value) {
    defaultProperties.put(key, value);
    conf = null;
  }

  public void addDefaultFlowStepStrategy(FlowStepStrategyFactory<JobConf> flowStepStrategyFactory) {
    defaultFlowStepStrategies.add(flowStepStrategyFactory);
  }

  public void addDefaultFlowStepStrategy(Class<? extends FlowStepStrategy<JobConf>> klass) {
    defaultFlowStepStrategies.add(new SimpleFlowStepStrategyFactory(klass));
  }

  public void clearDefaultFlowStepStrategies() {
    defaultFlowStepStrategies.clear();
  }

  public List<FlowStepStrategyFactory<JobConf>> getDefaultFlowStepStrategies() {
    List<FlowStepStrategyFactory<JobConf>> defaultStrategies = Lists.newArrayList();
    defaultStrategies.add(new SimpleFlowStepStrategyFactory(RenameJobStrategy.class));
    defaultStrategies.add(new SimpleFlowStepStrategyFactory(BloomAssemblyStrategy.class));
    return defaultStrategies;
  }

  public void addSerialization(Class<? extends Serialization> serialization) {
    serializations.add(serialization);
    conf = null;
  }

  protected void addRequiredProperty(String property){
    addInvalidPropertyValue(property, null);
  }

  protected void addInvalidPropertyValue(String property, String value) {
    invalidPropertyValues.put(property, value);
  }

  public void addSerializationToken(int token, Class<?> klass) {
    if (token < 128) {
      throw new IllegalArgumentException("Serialization tokens must be >= 128 (lower numbers are reserved by Cascading)");
    }

    if (serializationTokens.containsKey(token) && !serializationTokens.get(token).equals(klass)) {
      throw new IllegalArgumentException("Token " + token + " is already assigned to class " + serializationTokens.get(token));
    }

    serializationTokens.put(token, klass);
    conf = null;
  }

  private Map<String, String> getSerializationsProperty() {
    // Get the existing serializations
    List<String> strings = new ArrayList<String>();

    String existing = new JobConf().get("io.serializations");
    if (existing != null) {
      strings.add(existing);
    }

    // Append our custom serializations
    for (Class<? extends Serialization> klass : serializations) {
      strings.add(klass.getName());
    }

    return Collections.singletonMap("io.serializations", StringUtils.join(strings, ","));
  }

  private Map<String, String> getSerializationTokensProperty() {
    List<String> strings = new ArrayList<String>();
    for (Map.Entry<Integer, Class<?>> entry : serializationTokens.entrySet()) {
      strings.add(entry.getKey() + "=" + entry.getValue().getName());
    }
    if (strings.isEmpty()) {
      return Collections.emptyMap();
    } else {
      return Collections.singletonMap("cascading.serialization.tokens", StringUtils.join(strings, ","));
    }
  }

  public Map<Object, Object> getDefaultProperties() {
    Map<Object, Object> properties = new HashMap<Object, Object>();
    properties.putAll(getDefaultSerializationProperties());
    properties.putAll(defaultProperties);
    return properties;
  }

  public Map<Object, Object> getDefaultSerializationProperties() {
    Map<Object, Object> properties = new HashMap<Object, Object>();
    properties.putAll(getSerializationsProperty());
    properties.putAll(getSerializationTokensProperty());
    return properties;
  }

  public JobConf getJobConf() {
    if (conf == null) {
      conf = new JobConf();
      setAll(conf, getSerializationsProperty());
      setAll(conf, getSerializationTokensProperty());
    }
    return new JobConf(conf);
  }

  public JobConf getJobConfWithDefaultProperties() {
    JobConf jobConf = getJobConf();

    for (Map.Entry<Object, Object> entry : getDefaultProperties().entrySet()) {
      jobConf.set(entry.getKey().toString(), entry.getValue().toString());
    }

    return jobConf;
  }

  public FlowConnector getFlowConnector() {
    return buildFlowConnector(Collections.emptyMap(), Collections.<FlowStepStrategy<JobConf>>emptyList());
  }

  public FlowConnector getFlowConnector(Map<Object, Object> properties) {
    return buildFlowConnector(properties, Collections.<FlowStepStrategy<JobConf>>emptyList());
  }

  public FlowConnector getFlowConnector(List<FlowStepStrategy<JobConf>> flowStepStrategies) {
    return buildFlowConnector(Collections.emptyMap(), flowStepStrategies);
  }

  public FlowConnector getFlowConnector(Map<Object, Object> properties,
                                        List<FlowStepStrategy<JobConf>> flowStepStrategies) {
    return buildFlowConnector(properties, flowStepStrategies);
  }

  private FlowConnector buildFlowConnector(Map<Object, Object> properties,
                                           List<FlowStepStrategy<JobConf>> flowStepStrategies) {

    List<FlowStepStrategy<JobConf>> strategies = Lists.newArrayList(flowStepStrategies);
    strategies.addAll(resolveFlowStepStrategies());

    return buildFlowConnector(getJobConf(),
        combineProperties(getDefaultProperties(), properties),
        strategies,
        invalidPropertyValues);
  }

  public Multimap<String, String> getInvalidPropertyValues() {
    return invalidPropertyValues;
  }

  public List<FlowStepStrategy<JobConf>> resolveFlowStepStrategies(){
    List<FlowStepStrategy<JobConf>> strategies = Lists.newArrayList();
    for (FlowStepStrategyFactory<JobConf> factory : defaultFlowStepStrategies) {
      strategies.add(factory.getFlowStepStrategy());
    }
    return strategies;
  }


  public static FlowConnector buildFlowConnector(JobConf jobConf,
                                                 Map<Object, Object> properties,
                                                 List<FlowStepStrategy<JobConf>> flowStepStrategies,
                                                 Multimap<String, String> invalidPropertyValues) {

    return buildFlowConnector(jobConf,
        new JobPersister.NoOp(),
        properties,
        flowStepStrategies,
        invalidPropertyValues);
  }

  // We extract this method so that the default name based on the stack position makes sense
  public static FlowConnector buildFlowConnector(JobConf jobConf,
                                                 JobPersister persister,
                                                 Map<Object, Object> properties,
                                                 List<FlowStepStrategy<JobConf>> flowStepStrategies,
                                                 Multimap<String, String> invalidPropertyValues) {

    //  check required against stuff loaded from app-site.xml
    for (Map.Entry<String, String> entry : invalidPropertyValues.entries()) {
      String property = entry.getKey();
      String value = entry.getValue();

      if (ObjectUtils.equals(resolveProperty(property, properties, jobConf), value)) {
        LOG.error("Property "+property+" set to invalid value "+value+" with properties map: "+properties);
        throw new RuntimeException("Cannot build flow without setting property: " + property +" to a value which is not "+value);
      }
    }

    return new LoggingFlowConnector(properties,
        new MultiFlowStepStrategy(flowStepStrategies),
        persister,
        OperationStatsUtils.formatStackPosition(OperationStatsUtils.getStackPosition(2)));
  }

  private static Object resolveProperty(String key, Map<Object, Object> properties, JobConf conf){
    if(properties.containsKey(key)){
      return properties.get(key);
    }
    return conf.get(key);
  }

  private static Map<Object, Object> combineProperties(Map<Object, Object> defaultProperties, Map<Object, Object> properties){
    Map<Object, Object> combinedProperties = Maps.newHashMap(defaultProperties);
    combinedProperties.putAll(properties);
    return combinedProperties;
  }

  public FlowProcess<JobConf> getFlowProcess() {
    return getFlowProcess(getJobConf());
  }

  public FlowProcess<JobConf> getFlowProcessWithDefaultProperties() {
    return getFlowProcess(getJobConfWithDefaultProperties());
  }


  public FlowProcess<JobConf> getFlowProcess(JobConf jobConf) {
    return new HadoopFlowProcess(jobConf);
  }

  private void setAll(Configuration conf, Map<String, String> properties) {
    for (Map.Entry<String, String> property : properties.entrySet()) {
      conf.set(property.getKey(), property.getValue());
    }
  }
}
