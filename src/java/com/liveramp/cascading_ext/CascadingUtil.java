package com.liveramp.cascading_ext;

import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.FlowStepStrategy;
import cascading.flow.hadoop.HadoopFlowProcess;
import com.liveramp.cascading_ext.bloom.BloomAssemblyStrategyFactory;
import com.liveramp.cascading_ext.bloom.BloomConstants;
import com.liveramp.cascading_ext.flow.LoggingFlowConnector;
import com.liveramp.cascading_ext.flow_step_strategy.FlowStepStrategyFactory;
import com.liveramp.cascading_ext.flow_step_strategy.MultiFlowStepStrategy;
import com.liveramp.cascading_ext.flow_step_strategy.RenameJobStrategy;
import com.liveramp.cascading_ext.flow_step_strategy.SimpleFlowStepStrategyFactory;
import com.liveramp.cascading_ext.hash2.HashFunction;
import com.liveramp.cascading_ext.hash2.HashFunctionFactory;
import com.liveramp.cascading_ext.hash2.murmur.Murmur64HashFactory;
import com.liveramp.cascading_ext.serialization.MapSerialization;
import org.apache.commons.collections.BidiMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.mapred.JobConf;

import java.util.*;

public class CascadingUtil {
  private static final CascadingUtil INSTANCE = new CascadingUtil();

  public static CascadingUtil get() {
    return INSTANCE;
  }

  protected CascadingUtil() {
    addDefaultFlowStepStrategy(RenameJobStrategy.class);
    addDefaultFlowStepStrategy(new BloomAssemblyStrategyFactory(BloomConstants.DEFAULT_HASH_FACTORY));
    addSerialization(MapSerialization.class);
    addHashFunctionToken(1, new Murmur64HashFactory());
  }

  private final Map<Object, Object> defaultProperties = new HashMap<Object, Object>();
  private final List<FlowStepStrategyFactory<JobConf>> defaultFlowStepStrategies = new ArrayList<FlowStepStrategyFactory<JobConf>>();
  private final Set<Class<? extends Serialization>> serializations = new HashSet<Class<? extends Serialization>>();
  private final Map<Integer, Class<?>> serializationTokens = new HashMap<Integer, Class<?>>();
  private final Map<Integer, Class<? extends HashFunctionFactory>> hashFactoryTokens = new HashMap<Integer, Class<? extends HashFunctionFactory>>();

  private transient JobConf conf = null;

  public void setDefaultProperty(Object key, Object value){
    defaultProperties.put(key, value);
    conf = null;
  }

  public void addDefaultFlowStepStrategy(FlowStepStrategyFactory<JobConf> flowStepStrategyFactory){
    defaultFlowStepStrategies.add(flowStepStrategyFactory);
  }

  public void addDefaultFlowStepStrategy(Class<? extends FlowStepStrategy<JobConf>> klass){
    defaultFlowStepStrategies.add(new SimpleFlowStepStrategyFactory(klass));
  }

  public void addSerialization(Class<? extends Serialization> serialization){
    serializations.add(serialization);
    conf = null;
  }

  public void addSerializationToken(int token, Class<?> klass){
    if(token < 128)
      throw new IllegalArgumentException("Serialization tokens must be >= 128 (lower numbers are reserved by Cascading)");

    if(serializationTokens.containsKey(token) && !serializationTokens.get(token).equals(klass))
      throw new IllegalArgumentException("Token " + token + " is already assigned to class " + serializationTokens.get(token));

    serializationTokens.put(token, klass);
  }

  public void addHashFunctionToken(int token, HashFunctionFactory factory){
    hashFactoryTokens.put(token, factory.getClass());
  }


  private Map<String, String> getSerializationsProperty(){
    // Get the existing serializations
    List<String> strings = new ArrayList<String>();

    String existing = new JobConf().get("io.serializations");
    if(existing != null)
      strings.add(existing);

    // Append our custom serializations
    for(Class<? extends Serialization> klass : serializations){
      strings.add(klass.getName());
    }

    return Collections.singletonMap("io.serializations", StringUtils.join(strings, ","));
  }

  private Map<String, String> getSerializationTokensProperty(){
    List<String> strings = new ArrayList<String>();
    for(Map.Entry<Integer, Class<?>> entry : serializationTokens.entrySet()){
      strings.add(entry.getKey() + "=" + entry.getValue().getName());
    }
    if(strings.isEmpty()){
      return Collections.emptyMap();
    }else{
      return Collections.singletonMap("cascading.serialization.tokens", StringUtils.join(strings, ","));
    }
  }

  private Map<String, String> getHashFunctionsProperty(){
    List<String> strings = new ArrayList<String>();
    for(Map.Entry<Integer, Class<? extends HashFunctionFactory>> entry : hashFactoryTokens.entrySet()){
      strings.add(entry.getKey() + "=" + entry.getValue().getName());
    }
    return Collections.singletonMap("hash.function.tokens", StringUtils.join(strings, ","));
  }


  public Map<Object, Object> getDefaultProperties(){
    Map<Object, Object> properties = new HashMap<Object, Object>();
    properties.putAll(getSerializationsProperty());
    properties.putAll(getHashFunctionsProperty());
    properties.putAll(getSerializationTokensProperty());
    properties.putAll(defaultProperties);
    return properties;
  }

  public JobConf getJobConf(){
    if(conf == null){
      conf = new JobConf();
      setAll(conf, getSerializationsProperty());
      setAll(conf, getHashFunctionsProperty());
      setAll(conf, getSerializationTokensProperty());
    }
    return new JobConf(conf);
  }

  public FlowConnector getFlowConnector() {
    return getFlowConnector(Collections.<Object, Object>emptyMap());
  }

  public FlowConnector getFlowConnector(Map<Object, Object> properties) {
    return getFlowConnector(properties, Collections.<FlowStepStrategy<JobConf>>emptyList());
  }

  public FlowConnector getFlowConnector(List<FlowStepStrategy<JobConf>> flowStepStrategies) {
    return getFlowConnector(Collections.<Object, Object>emptyMap(), flowStepStrategies);
  }

  public FlowConnector getFlowConnector(Map<Object, Object> properties, List<FlowStepStrategy<JobConf>> flowStepStrategies) {
    //Add in default properties
    Map<Object, Object> combinedProperties = getDefaultProperties();
    combinedProperties.putAll(properties);

    //Add in default flow step strategies
    List<FlowStepStrategy<JobConf>> combinedStrategies = new ArrayList<FlowStepStrategy<JobConf>>(flowStepStrategies);
    for(FlowStepStrategyFactory<JobConf> flowStepStrategyFactory : defaultFlowStepStrategies){
      combinedStrategies.add(flowStepStrategyFactory.getFlowStepStrategy());
    }

    return new LoggingFlowConnector(combinedProperties, new MultiFlowStepStrategy(combinedStrategies));
  }

  public FlowProcess<JobConf> getFlowProcess() {
    return getFlowProcess(getJobConf());
  }

  public FlowProcess<JobConf> getFlowProcess(JobConf jobConf) {
    return new HadoopFlowProcess(jobConf);
  }

  private void setAll(Configuration conf, Map<String, String> properties){
    for(Map.Entry<String, String> property: properties.entrySet()){
      conf.set(property.getKey(), property.getValue());
    }
  }
}