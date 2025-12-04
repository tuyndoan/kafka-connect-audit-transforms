/*
 * Copyright © 2025 Tuyn Doan (doanvantuyn@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tuyndv.kafka.smt;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InsertField<R extends ConnectRecord<R>> implements Transformation<R> {
  private static final Logger log = LoggerFactory.getLogger(InsertField.class);
  public static final String OVERVIEW_DOC = "Insert field(s) using attributes from the record metadata or a configured static value with datatype.<p/>Use the concrete transformation type designed for the record key (<code>" + Key.class
    .getName() + "</code>) or value (<code>" + Value.class
    .getName() + "</code>).";
  
  private static final String OPTIONALITY_DOC = "Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).";
  
  public static final ConfigDef CONFIG_DEF = (new ConfigDef())
    .define("topic.field", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for Kafka topic. Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).")
    
    .define("partition.field", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for Kafka partition. Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).")
    
    .define("offset.field", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for Kafka offset - only applicable to sink connectors.<br/>Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).")
    
    .define("timestamp.field", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for record timestamp. Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).")
    
    .define("static.field", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for static data field. Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).")
    
    .define("static.type", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Field name for static type field. ")
    
    .define("static.value", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM, "Static field value, if field name configured.");
  
  private static final String PURPOSE = "field insertion";
  
  private static final Schema OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build();
  
  private InsertionSpec topicField;
  
  private InsertionSpec partitionField;
  
  private InsertionSpec offsetField;
  
  private InsertionSpec timestampField;
  
  private InsertionSpec staticField;
  
  private String staticType;
  
  private String staticValue;
  
  private Cache<Schema, Schema> schemaUpdateCache;
  
  private static interface ConfigName {
    public static final String TOPIC_FIELD = "topic.field";
    
    public static final String PARTITION_FIELD = "partition.field";
    
    public static final String OFFSET_FIELD = "offset.field";
    
    public static final String TIMESTAMP_FIELD = "timestamp.field";
    
    public static final String STATIC_FIELD = "static.field";
    
    public static final String STATIC_TYPE = "static.type";
    
    public static final String STATIC_VALUE = "static.value";
  }
  
  private static final class InsertionSpec {
    final String name;
    
    final boolean optional;
    
    private InsertionSpec(String name, boolean optional) {
      this.name = name;
      this.optional = optional;
    }
    
    public static InsertionSpec parse(String spec) {
      if (spec == null)
        return null; 
      if (spec.endsWith("?"))
        return new InsertionSpec(spec.substring(0, spec.length() - 1), true); 
      if (spec.endsWith("!"))
        return new InsertionSpec(spec.substring(0, spec.length() - 1), false); 
      return new InsertionSpec(spec, true);
    }
  }
  
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    this.topicField = InsertionSpec.parse(config.getString("topic.field"));
    this.partitionField = InsertionSpec.parse(config.getString("partition.field"));
    this.offsetField = InsertionSpec.parse(config.getString("offset.field"));
    this.timestampField = InsertionSpec.parse(config.getString("timestamp.field"));
    this.staticField = InsertionSpec.parse(config.getString("static.field"));
    this.staticType = config.getString("static.type");
    this.staticValue = config.getString("static.value");
    
    if (this.topicField == null && this.partitionField == null && this.offsetField == null && this.timestampField == null && this.staticField == null) {
      throw new ConfigException("No field insertion configured. At least one field must be configured.");
    }
    
    if (this.staticField != null && this.staticValue == null) {
      throw new ConfigException("static.value", null, "No value specified for static field: " + this.staticField.name);
    }
    
    if (this.staticField != null && (this.staticType == null || this.staticType.trim().isEmpty())) {
      log.warn("static.type not specified for static field '{}', defaulting to 'string'", this.staticField.name);
      this.staticType = "string";
    }
    
    this.schemaUpdateCache = (Cache<Schema, Schema>)new SynchronizedCache((Cache)new LRUCache(16));
    
    log.debug("InsertField configured - topic: {}, partition: {}, offset: {}, timestamp: {}, static: {}",
        this.topicField != null ? this.topicField.name : null,
        this.partitionField != null ? this.partitionField.name : null,
        this.offsetField != null ? this.offsetField.name : null,
        this.timestampField != null ? this.timestampField.name : null,
        this.staticField != null ? this.staticField.name : null);
  }
  
  private static <T> T castToType(String value, String typeName) {
    if (value == null) {
      return null;
    }
    
    if (typeName == null || typeName.trim().isEmpty()) {
      typeName = "string";
    }
    
    try {
      switch (typeName.toLowerCase().trim()) {
        case "byte":
        case "int8":
          return (T)Byte.valueOf(value);
        case "short":
        case "int16":
          return (T)Short.valueOf(value);
        case "int":
        case "int32":
          return (T)Integer.valueOf(value);
        case "long":
        case "int64":
          return (T)Long.valueOf(value);
        case "float":
        case "float32":
          return (T)Float.valueOf(value);
        case "double":
        case "float64":
          return (T)Double.valueOf(value);
        case "boolean":
          return (T)Boolean.valueOf(value);
        case "string":
          return (T)value;
        default:
          log.warn("Unsupported type '{}', defaulting to string", typeName);
          return (T)value;
      }
    } catch (NumberFormatException e) {
      log.error("Failed to cast value '{}' to type '{}': {}", value, typeName, e.getMessage());
      throw new IllegalArgumentException("Failed to cast value '" + value + "' to type '" + typeName + "': " + e.getMessage(), e);
    }
  }
  
  public R apply(R record) {
    if (operatingValue(record) == null)
      return record; 
    if (operatingSchema(record) == null)
      return applySchemaless(record); 
    return applyWithSchema(record);
  }
  
  private R applySchemaless(R record) {
    try {
      Map<String, Object> value = Requirements.requireMap(operatingValue(record), "field insertion");
      Map<String, Object> updatedValue = new HashMap<>(value);
      
      if (this.topicField != null) {
        String topic = record.topic();
        if (topic != null || !this.topicField.optional) {
          updatedValue.put(this.topicField.name, topic);
        }
      }
      
      if (this.partitionField != null && record.kafkaPartition() != null) {
        updatedValue.put(this.partitionField.name, record.kafkaPartition());
      }
      
      if (this.offsetField != null) {
        try {
          Long offset = Requirements.requireSinkRecord((ConnectRecord)record, "field insertion").kafkaOffset();
          updatedValue.put(this.offsetField.name, offset);
        } catch (Exception e) {
          log.warn("Failed to extract offset for field '{}': {}", this.offsetField.name, e.getMessage());
          if (!this.offsetField.optional) {
            throw e;
          }
        }
      }
      
      if (this.timestampField != null && record.timestamp() != null) {
        updatedValue.put(this.timestampField.name, record.timestamp());
      }
      
      if (this.staticField != null && this.staticValue != null) {
        updatedValue.put(this.staticField.name, castToType(this.staticValue, this.staticType));
      }
      
      return newRecord(record, null, updatedValue);
    } catch (Exception e) {
      log.error("Error applying schemaless transformation. Topic: {}, Partition: {}", 
          record.topic(), record.kafkaPartition(), e);
      throw e;
    }
  }
  
  private R applyWithSchema(R record) {
    try {
      Struct value = Requirements.requireStruct(operatingValue(record), "field insertion");
      Schema updatedSchema = (Schema)this.schemaUpdateCache.get(value.schema());
      if (updatedSchema == null) {
        updatedSchema = makeUpdatedSchema(value.schema());
        this.schemaUpdateCache.put(value.schema(), updatedSchema);
      } 
      
      Struct updatedValue = new Struct(updatedSchema);
      // Copy existing fields
      for (Field field : value.schema().fields()) {
        updatedValue.put(field.name(), value.get(field));
      }
      
      // Add new fields
      if (this.topicField != null) {
        String topic = record.topic();
        if (topic != null || !this.topicField.optional) {
          updatedValue.put(this.topicField.name, topic);
        }
      }
      
      if (this.partitionField != null && record.kafkaPartition() != null) {
        updatedValue.put(this.partitionField.name, record.kafkaPartition());
      }
      
      if (this.offsetField != null) {
        try {
          Long offset = Requirements.requireSinkRecord((ConnectRecord)record, "field insertion").kafkaOffset();
          updatedValue.put(this.offsetField.name, offset);
        } catch (Exception e) {
          log.warn("Failed to extract offset for field '{}': {}", this.offsetField.name, e.getMessage());
          if (!this.offsetField.optional) {
            throw e;
          }
        }
      }
      
      if (this.timestampField != null && record.timestamp() != null) {
        updatedValue.put(this.timestampField.name, new Date(record.timestamp().longValue()));
      }
      
      if (this.staticField != null && this.staticValue != null) {
        updatedValue.put(this.staticField.name, castToType(this.staticValue, this.staticType));
      }
      
      return newRecord(record, updatedSchema, updatedValue);
    } catch (Exception e) {
      log.error("Error applying schema transformation. Topic: {}, Partition: {}", 
          record.topic(), record.kafkaPartition(), e);
      throw e;
    }
  }
  
  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields())
      builder.field(field.name(), field.schema()); 
    if (this.topicField != null)
      builder.field(this.topicField.name, this.topicField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA); 
    if (this.partitionField != null)
      builder.field(this.partitionField.name, 
          this.partitionField.optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA); 
    if (this.offsetField != null)
      builder.field(this.offsetField.name, this.offsetField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA); 
    if (this.timestampField != null)
      builder.field(this.timestampField.name, this.timestampField.optional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA); 
    if (this.staticField != null) {
      switch (this.staticType) {
        case "int8":
          builder.field(this.staticField.name, 
              this.staticField.optional ? Schema.OPTIONAL_INT8_SCHEMA : Schema.INT8_SCHEMA);
          return builder.build();
        case "int16":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_INT16_SCHEMA : Schema.INT16_SCHEMA);
          return builder.build();
        case "int32":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
          return builder.build();
        case "int64":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
          return builder.build();
        case "float32":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_FLOAT32_SCHEMA : Schema.FLOAT32_SCHEMA);
          return builder.build();
        case "float64":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_FLOAT64_SCHEMA : Schema.FLOAT64_SCHEMA);
          return builder.build();
        case "boolean":
          builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_BOOLEAN_SCHEMA : Schema.BOOLEAN_SCHEMA);
          return builder.build();
      } 
      builder.field(this.staticField.name, this.staticField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
    } 
    return builder.build();
  }
  
  public void close() {
    if (this.schemaUpdateCache != null) {
      this.schemaUpdateCache = null;
      log.debug("InsertField closed and cache cleared");
    }
  }
  
  public ConfigDef config() {
    return CONFIG_DEF;
  }
  
  protected abstract Schema operatingSchema(R paramR);
  
  protected abstract Object operatingValue(R paramR);
  
  protected abstract R newRecord(R paramR, Schema paramSchema, Object paramObject);
  
  public static class Key<R extends ConnectRecord<R>> extends InsertField<R> {
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }
    
    protected Object operatingValue(R record) {
      return record.key();
    }
    
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return (R)record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record
          .valueSchema(), record.value(), record.timestamp());
    }
  }
  
  public static class Value<R extends ConnectRecord<R>> extends InsertField<R> {
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }
    
    protected Object operatingValue(R record) {
      return record.value();
    }
    
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return (R)record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record
          .timestamp());
    }
  }
}
