package com.liveramp.cascading_ext;

import com.liveramp.cascading_ext.helpers.StringHelper;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.mapred.JobConf;

import java.util.*;

public class CascadingHelper {
  private static final CascadingHelper instance = new CascadingHelper();

  public static CascadingHelper get() {
    return instance;
  }

  private CascadingHelper(){}

  private final Set<Class<? extends Serialization>> serializations = new HashSet<Class<? extends Serialization>>();
  private final Map<Integer, Class<?>> serializationTokens = new HashMap<Integer, Class<?>>();
  private final Map<String, String> defaultProperties = new HashMap<String, String>();
  private transient JobConf conf = null;

  protected void setDefaultProperty(String key, String value){
    defaultProperties.put(key, value);
    conf = null;
  }

  protected void addSerialization(Class<? extends Serialization> serialization){
    serializations.add(serialization);
    conf = null;
  }

  protected void addSerializationToken(int token, Class<?> klass){
    if(token <= 128)
      throw new IllegalArgumentException("Serialization tokens must be greater than 128 (lower numbers are reserved by Cascading)");

    if(serializationTokens.containsKey(token) && !serializationTokens.get(token).equals(klass))
      throw new IllegalArgumentException("Token " + token + " is already assigned to class " + serializationTokens.get(token));

    serializationTokens.put(token, klass);
  }

  public JobConf getJobConf(){
    if(conf == null){
      conf = new JobConf();

      // Get the existing serializations
      List<String> strings = new ArrayList<String>();
      String existing = conf.get("io.serializations");
      if(existing != null)
        strings.add(existing);

      // Append our custom serializations
      for(Class<? extends Serialization> klass : serializations){
        strings.add(klass.getName());
      }

      conf.set("io.serializations", StringHelper.join(strings, ","));

      // Set serialization tokens
      strings = new ArrayList<String>();
      for(Map.Entry<Integer, Class<?>> entry : serializationTokens.entrySet()){
        strings.add(entry.getKey() + "=" + entry.getValue().getName());
      }

      conf.set("cascading.serialization.tokens", StringHelper.join(strings, ","));

      // Set all of the default properties
      for(Map.Entry<String, String> entry : defaultProperties.entrySet()){
        conf.set(entry.getKey(), entry.getValue());
      }
    }
    return new JobConf(conf);
  }
}