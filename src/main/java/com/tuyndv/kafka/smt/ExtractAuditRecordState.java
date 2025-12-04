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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ExtractAuditRecordState<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {
  private static final Logger log = LoggerFactory.getLogger(ExtractAuditRecordState.class);
  
  private static final String DATE_TIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
  
  private static final String DEFAULT_TIMEZONE = "UTC";
  
  private static final Pattern SCHEMA_TYPE_PATTERN = Pattern.compile("\\{([^:]+)");
  
  private static final ConfigDef CONFIG_DEF = (new ConfigDef())
    .define("exclude", ConfigDef.Type.LIST, Collections.singletonList("Timestamp"), ConfigDef.Importance.MEDIUM, "Fields to exclude. This takes precedence over the fields to include. Default: Timestamp")
    
    .define("include", ConfigDef.Type.LIST, Collections.emptyList(), ConfigDef.Importance.MEDIUM, "Fields to include. If specified, only these fields will be used.")
    
    .define("timezone", ConfigDef.Type.STRING, "UTC", ConfigDef.Importance.MEDIUM, "TimeZone: UTC");
  
  private Set<String> exclude;
  
  private Set<String> include;
  
  private String timezone;
  
  private ObjectMapper objectMapper;
  
  private DateTimeFormatter dateTimeFormatter;
  
  private Schema updatedSchema;
  
  public String version() {
    return AppInfoParser.getVersion();
  }
  
  public void configure(Map<String, ?> configs) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
    this.exclude = new HashSet<>(config.getList("exclude"));
    this.include = new HashSet<>(config.getList("include"));
    this.timezone = config.getString("timezone");
    if (this.timezone == null || this.timezone.isEmpty())
      this.timezone = DEFAULT_TIMEZONE;
    
    // Log configuration for debugging
    if (log.isDebugEnabled()) {
      log.debug("ExtractAuditRecordState configured - exclude: {}, include: {}, timezone: {}", 
          this.exclude, this.include, this.timezone);
    }
    if (!this.exclude.isEmpty()) {
      log.info("Exclude list configured with {} fields: {}", this.exclude.size(), this.exclude);
    }
    if (!this.include.isEmpty()) {
      log.info("Include list configured with {} fields: {}", this.include.size(), this.include);
    }
    
    // Initialize ObjectMapper once
    this.objectMapper = new ObjectMapper();
    
    // Initialize DateTimeFormatter once (thread-safe)
    ZoneId zoneId = ZoneId.of(this.timezone);
    this.dateTimeFormatter = DateTimeFormatter.ofPattern(DATE_TIME_FORMAT).withZone(zoneId);
    
    // Cache schema
    this.updatedSchema = makeUpdatedSchema();
  }
  
  boolean filter(String fieldName) {
    // Exclude list takes precedence - always exclude if in exclude list
    if (this.exclude.contains(fieldName)) {
      return false;
    }
    // Include list is only used for FieldIncludes (unchanged fields tracking)
    // For NewValues/OriginalValues, we include all fields (except excluded ones)
    // This allows include list to track specific unchanged fields while still
    // capturing all changed fields in NewValues/OriginalValues
    return true;
  }
  
  boolean shouldIncludeInFieldIncludes(String fieldName) {
    // FieldIncludes only tracks fields that are in include list
    return !this.include.isEmpty() && this.include.contains(fieldName);
  }
  
  private Schema makeUpdatedSchema() {
    SchemaBuilder builder = SchemaBuilder.struct();
    // Primary identifier
    builder.field("id", Schema.STRING_SCHEMA);
    
    // Kafka metadata
    builder.field("topic", Schema.STRING_SCHEMA);
    builder.field("partition", Schema.INT32_SCHEMA);
    builder.field("offset", Schema.OPTIONAL_INT64_SCHEMA);
    
    // Change information
    builder.field("change_type", Schema.STRING_SCHEMA);
    builder.field("change_time", Schema.INT64_SCHEMA).required(); // Int64 (epoch milliseconds) for ClickHouse DateTime64(3) compatibility with JSON format
    builder.field("sync_date", Schema.INT64_SCHEMA).required(); // Int64 (epoch milliseconds) for ClickHouse DateTime64(3) compatibility with JSON format
    
    // Data fields
    builder.field("primary_keys", Schema.STRING_SCHEMA);
    builder.field("new_values", Schema.STRING_SCHEMA);
    builder.field("original_values", Schema.STRING_SCHEMA);
    builder.field("field_includes", Schema.STRING_SCHEMA);
    
    // Source database information
    builder.field("database", Schema.STRING_SCHEMA);
    builder.field("schema", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("table", Schema.STRING_SCHEMA);
    
    // CDC source metadata
    builder.field("connector", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("change_lsn", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("commit_lsn", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("event_serial_no", Schema.OPTIONAL_INT64_SCHEMA);
    builder.field("snapshot", Schema.OPTIONAL_STRING_SCHEMA);
    
    return builder.build();
  }
  
  private String formatDate(Date date) {
    if (date == null)
      return ""; 
    Instant instant = date.toInstant();
    return this.dateTimeFormatter.format(instant);
  }
  
  private String formatTime(Date date) {
    if (date == null)
      return ""; 
    // Format time only (HH:mm:ss.SSS)
    DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
        .withZone(ZoneId.of(this.timezone));
    Instant instant = date.toInstant();
    return timeFormatter.format(instant);
  }
  
  private String formatTimestamp(Date date) {
    if (date == null)
      return ""; 
    Instant instant = date.toInstant();
    return this.dateTimeFormatter.format(instant);
  }
  
  private String formatTimestamp(Long epochMilliSeconds) {
    if (epochMilliSeconds == null)
      return ""; 
    Instant instant = Instant.ofEpochMilli(epochMilliSeconds);
    return this.dateTimeFormatter.format(instant);
  }
  
  public Object convertToUTCString(Field field, Object value, String type) {
    switch (type) {
      case "org.apache.kafka.connect.data.Date":
        return formatDate((Date)value);
      case "org.apache.kafka.connect.data.Time":
        return formatTime((Date)value);
      case "org.apache.kafka.connect.data.Timestamp":
        return formatTimestamp((Date)value);
    } 
    return value;
  }
  
  public R apply(R record) {
    if (record.value() == null)
      return record; 
    try {
      Struct auditValue = new Struct(this.updatedSchema);
      Map<String, Object> afterValues = new HashMap<>();
      Map<String, Object> beforeValues = new HashMap<>();
      Map<String, Object> keyValues = new HashMap<>();
      Map<String, Object> fieldIncludes = new HashMap<>();
      List<String> keyFields = new ArrayList<>();
      Map<String, String> schemaTypes = new HashMap<>();
      
      // Set change_time to current time as default (required field)
      // Will be overridden by source.ts_ms if available
      Date currentTime = new Date();
      Long changeTimeMs = currentTime.getTime();
      auditValue.put("change_time", changeTimeMs);
      
      Struct value = Requirements.requireStruct(record.value(), "Record value should be struct.");
      
      // Extract schema types for before/after fields
      for (Field field : value.schema().fields()) {
        String fieldName = field.name();
        if (Objects.equals(fieldName, "before") || Objects.equals(fieldName, "after")) {
          Schema fieldSchema = field.schema();
          if (fieldSchema != null && fieldSchema.fields() != null) {
            for (Field f : fieldSchema.fields()) {
              Matcher matcher = SCHEMA_TYPE_PATTERN.matcher(f.schema().toString());
              if (matcher.find()) {
                schemaTypes.put(fieldName + "." + f.name(), matcher.group(1)); 
              }
            }
          }
        } 
      } 
      
      // Safely get before/after/source fields - check if they exist in schema first
      Object after = null;
      Object before = null;
      Object source = null;
      
      Field afterField = value.schema().field("after");
      if (afterField != null) {
        try {
          after = value.get("after");
        } catch (Exception e) {
          log.warn("Failed to get 'after' field from record, treating as null. Error: {}", e.getMessage());
        }
      }
      
      Field beforeField = value.schema().field("before");
      if (beforeField != null) {
        try {
          before = value.get("before");
        } catch (Exception e) {
          log.warn("Failed to get 'before' field from record, treating as null. Error: {}", e.getMessage());
        }
      }
      
      Field sourceField = value.schema().field("source");
      if (sourceField != null) {
        try {
          source = value.get("source");
        } catch (Exception e) {
          log.warn("Failed to get 'source' field from record, treating as null. Error: {}", e.getMessage());
        }
      }
      
      // Extract operation type from 'op' field (c=create/insert, u=update, d=delete)
      String op = null;
      Field opField = value.schema().field("op");
      if (opField != null) {
        try {
          Object opValue = value.get("op");
          if (opValue != null) {
            op = opValue.toString();
          }
        } catch (Exception e) {
          log.warn("Failed to get 'op' field from record. Error: {}", e.getMessage());
        }
      }
      
      // Extract key fields and values - handle null key gracefully
      if (record.key() != null) {
        Struct keyStruct = Requirements.requireStruct(record.key(), "Record key should be struct.");
        if (keyStruct.schema() != null && keyStruct.schema().fields() != null) {
          for (Field field : keyStruct.schema().fields()) {
            String fieldName = field.name();
            keyFields.add(fieldName);
            // Extract primary key values directly from record.key()
            try {
              Object keyValue = keyStruct.get(field);
              if (keyValue != null) {
                keyValues.put(fieldName, keyValue);
              }
            } catch (Exception e) {
              log.warn("Failed to extract primary key value for field '{}' from record key. Error: {}", fieldName, e.getMessage());
            }
          }
        }
      }
      
      // Extract source information
      if (source != null) {
        try {
          Struct sourceValue = Requirements.requireStruct(source, "Source value should be struct.");
          for (Field field : sourceValue.schema().fields()) {
            String fieldName = field.name();
            try {
              if ("table".equals(fieldName)) {
                Object tableValue = sourceValue.get(field);
                if (tableValue != null) {
                  auditValue.put("table", tableValue.toString());
                }
              } else if ("schema".equals(fieldName)) {
                Object schemaValue = sourceValue.get(field);
                if (schemaValue != null) {
                  auditValue.put("schema", schemaValue.toString());
                }
              } else if ("db".equals(fieldName) || "database".equals(fieldName)) {
                Object dbValue = sourceValue.get(field);
                if (dbValue != null) {
                  auditValue.put("database", dbValue.toString());
                }
              } else if ("connector".equals(fieldName)) {
                Object connectorValue = sourceValue.get(field);
                if (connectorValue != null) {
                  auditValue.put("connector", connectorValue.toString());
                }
              } else if ("change_lsn".equals(fieldName)) {
                Object changeLsnValue = sourceValue.get(field);
                if (changeLsnValue != null) {
                  auditValue.put("change_lsn", changeLsnValue.toString());
                }
              } else if ("commit_lsn".equals(fieldName)) {
                Object commitLsnValue = sourceValue.get(field);
                if (commitLsnValue != null) {
                  auditValue.put("commit_lsn", commitLsnValue.toString());
                }
              } else if ("event_serial_no".equals(fieldName)) {
                Object eventSerialNoValue = sourceValue.get(field);
                if (eventSerialNoValue != null) {
                  try {
                    Long eventSerialNo;
                    if (eventSerialNoValue instanceof Long) {
                      eventSerialNo = (Long) eventSerialNoValue;
                    } else if (eventSerialNoValue instanceof Number) {
                      eventSerialNo = ((Number) eventSerialNoValue).longValue();
                    } else {
                      eventSerialNo = sourceValue.getInt64(fieldName);
                    }
                    if (eventSerialNo != null) {
                      auditValue.put("event_serial_no", eventSerialNo);
                    }
                  } catch (Exception e) {
                    log.warn("Failed to extract event_serial_no from source, skipping. Error: {}", e.getMessage());
                  }
                }
              } else if ("snapshot".equals(fieldName)) {
                Object snapshotValue = sourceValue.get(field);
                if (snapshotValue != null) {
                  auditValue.put("snapshot", snapshotValue.toString());
                }
              } else if ("ts_ms".equals(fieldName)) {
                Object tsValue = sourceValue.get(field);
                if (tsValue != null) {
                  try {
                    Long timestamp;
                    if (tsValue instanceof Long) {
                      timestamp = (Long) tsValue;
                    } else if (tsValue instanceof Number) {
                      timestamp = ((Number) tsValue).longValue();
                    } else {
                      timestamp = sourceValue.getInt64(fieldName);
                    }
                    if (timestamp != null && timestamp > 0) {
                      // Use epoch milliseconds directly for Int64 schema - override default current time
                      auditValue.put("change_time", timestamp);
                      changeTimeMs = timestamp; // Update changeTimeMs variable (not used for sync_date anymore, but kept for consistency)
                    }
                  } catch (Exception e) {
                    // If extraction fails, keep the default current time set earlier
                    log.warn("Failed to extract timestamp from source.ts_ms field, using current time as default. Error: {}", e.getMessage(), e);
                  }
                }
              }
            } catch (Exception e) {
              // Log and continue processing other fields
              log.warn("Error processing source field '{}', skipping. Error: {}", fieldName, e.getMessage(), e);
            }
          }
        } catch (Exception e) {
          log.warn("Failed to process source information, skipping. Error: {}", e.getMessage(), e);
        }
      }
      // Determine operation type from 'op' field, fallback to before/after logic if op not available
      String action = "";
      if (op != null) {
        switch (op.toLowerCase()) {
          case "c":
            action = "Insert";
            break;
          case "u":
            action = "Update";
            break;
          case "d":
            action = "Delete";
            break;
          default:
            log.warn("Unknown operation type '{}', will infer from before/after fields", op);
        }
      }
      
      boolean isHasBefore = false, isHasAfter = false, isHasIncludes = false;
      
      // Process INSERT or UPDATE (when after is not null)
      if (after != null) {
        Struct afterValue = Requirements.requireStruct(after, "After value should be struct.");
        Struct beforeValue = (before != null) ? Requirements.requireStruct(before, "Before value should be struct.") : null;
        
        // If op not available, infer action from before/after
        if (action.isEmpty()) {
          action = (beforeValue != null) ? "Update" : "Insert";
        }
        
        // Collect all field names from both before and after to ensure we don't miss any fields
        Set<String> allFieldNames = new HashSet<>();
        if (afterValue.schema() != null && afterValue.schema().fields() != null) {
          for (Field field : afterValue.schema().fields()) {
            allFieldNames.add(field.name());
          }
        }
        if (beforeValue != null && beforeValue.schema() != null && beforeValue.schema().fields() != null) {
          for (Field field : beforeValue.schema().fields()) {
            allFieldNames.add(field.name());
          }
        }
        
        // Log total fields found
        if (log.isDebugEnabled()) {
          log.debug("Total fields found in schema: {} (after: {}, before: {})", 
              allFieldNames.size(),
              (afterValue.schema() != null && afterValue.schema().fields() != null) ? afterValue.schema().fields().size() : 0,
              (beforeValue != null && beforeValue.schema() != null && beforeValue.schema().fields() != null) ? beforeValue.schema().fields().size() : 0);
        }
        
        // Process each field
        for (String fieldName : allFieldNames) {
          if (!filter(fieldName)) {
            if (log.isDebugEnabled()) {
              log.debug("Field '{}' filtered out (excluded)", fieldName);
            }
            continue;
          }
          
          if (log.isDebugEnabled()) {
            log.debug("Processing field: {}", fieldName);
          }
          
          // Get field from schema (try after first, then before)
          Field afterFieldSchema = (afterValue.schema() != null) ? afterValue.schema().field(fieldName) : null;
          Field beforeFieldSchema = (beforeValue != null && beforeValue.schema() != null) ? beforeValue.schema().field(fieldName) : null;
          
          Object afterFieldValue = null;
          Object beforeFieldValue = null;
          
          // Extract after value (including null values)
          if (afterFieldSchema != null) {
            try {
              afterFieldValue = afterValue.get(afterFieldSchema);
              // Note: null values are valid and should be included
            } catch (Exception e) {
              log.warn("Failed to get 'after' field '{}', treating as null. Error: {}", fieldName, e.getMessage());
              afterFieldValue = null; // Explicitly set to null on error
            }
          }
          
          // Extract before value
          if (beforeFieldSchema != null) {
            try {
              beforeFieldValue = beforeValue.get(beforeFieldSchema);
            } catch (Exception e) {
              log.warn("Failed to get 'before' field '{}', treating as null. Error: {}", fieldName, e.getMessage());
            }
          }
          
          String afterFieldType = schemaTypes.get("after." + fieldName);
          String beforeFieldType = schemaTypes.get("before." + fieldName);
          boolean isAfterDate = (afterFieldType != null && (afterFieldType.contains("Time") || afterFieldType.contains("Date")));
          boolean isBeforeDate = (beforeFieldType != null && (beforeFieldType.contains("Time") || beforeFieldType.contains("Date")));
          
          // For UPDATE: compare with before value to find changed fields
          if ("Update".equals(action) && beforeValue != null) {
            if (!Objects.equals(afterFieldValue, beforeFieldValue)) {
              // Field changed - add to both beforeValues and afterValues
              // Always add both, even if one is null (e.g., null -> value or value -> null)
              Object beforeValueToAdd = beforeFieldValue;
              if (beforeFieldValue != null && isBeforeDate) {
                beforeValueToAdd = convertToUTCString(beforeFieldSchema, beforeFieldValue, beforeFieldType);
              }
              beforeValues.put(fieldName, beforeValueToAdd);
              isHasBefore = true;
              
              Object afterValueToAdd = afterFieldValue;
              if (afterFieldValue != null && isAfterDate) {
                afterValueToAdd = convertToUTCString(afterFieldSchema, afterFieldValue, afterFieldType);
              }
              afterValues.put(fieldName, afterValueToAdd);
              isHasAfter = true;
            } else {
              // Field unchanged - only track in FieldIncludes if in include list
              if (shouldIncludeInFieldIncludes(fieldName) && afterFieldValue != null) {
                // Field unchanged but in include list - track it in FieldIncludes
                fieldIncludes.put(fieldName, isAfterDate ? convertToUTCString(afterFieldSchema, afterFieldValue, afterFieldType) : afterFieldValue);
                isHasIncludes = true;
              }
              // For UPDATE: unchanged fields are NOT added to NewValues/OriginalValues
              // Only changed fields appear in NewValues/OriginalValues
            }
          } else {
            // For INSERT: add all fields to afterValues (excluding null values)
            // Check if field exists in after schema and has non-null value
            if (afterFieldSchema != null && afterFieldValue != null) {
              Object valueToAdd = afterFieldValue;
              if (isAfterDate) {
                valueToAdd = convertToUTCString(afterFieldSchema, afterFieldValue, afterFieldType);
              }
              // Only add field if value is not null
              afterValues.put(fieldName, valueToAdd);
              isHasAfter = true;
            } else if (afterFieldSchema == null) {
              // Field not in after schema - this should not happen for INSERT
              log.warn("INSERT operation: Field '{}' not found in after schema. Available fields: {}", 
                  fieldName, 
                  (afterValue.schema() != null && afterValue.schema().fields() != null) 
                      ? afterValue.schema().fields().stream().map(Field::name).collect(java.util.stream.Collectors.toList())
                      : "null");
            }
            // Skip fields with null values in INSERT - they are not added to NewValues
          }
        } 
      } else if (before != null) {
        // Process DELETE (when before is not null and after is null)
        if (action.isEmpty()) {
          action = "Delete";
        }
        
        Struct beforeValue = Requirements.requireStruct(before, "Before value should be struct.");
        for (Field field : beforeValue.schema().fields()) {
          String fieldName = field.name();
          if (!filter(fieldName)) {
            continue;
          }
          
          Object beforeFieldValue = beforeValue.get(field);
          
          // Note: Primary keys are already extracted from record.key() above
          // This check is kept for backward compatibility but keyValues should already be populated
          
          String beforeFieldType = schemaTypes.get("before." + fieldName);
          boolean isBeforeDate = (beforeFieldType != null && (beforeFieldType.contains("Time") || beforeFieldType.contains("Date")));
          
          beforeValues.put(fieldName, isBeforeDate ? convertToUTCString(field, beforeFieldValue, beforeFieldType) : beforeFieldValue);
          isHasBefore = true;
        } 
      } 
      auditValue.put("id", UUID.randomUUID().toString());
      auditValue.put("change_type", action.isEmpty() ? "" : action.toUpperCase());
      auditValue.put("topic", record.topic() != null ? record.topic() : "");
      auditValue.put("partition", Integer.valueOf((record.kafkaPartition() != null) ? record.kafkaPartition().intValue() : 0));
      auditValue.put("primary_keys", this.objectMapper.writeValueAsString(keyValues));
      
      // Always serialize maps, even if empty (will return "{}")
      auditValue.put("new_values", this.objectMapper.writeValueAsString(afterValues));
      auditValue.put("original_values", this.objectMapper.writeValueAsString(beforeValues));
      auditValue.put("field_includes", this.objectMapper.writeValueAsString(fieldIncludes));
      
      // Extract offset from sink record (if available)
      Long offset = null;
      if (record instanceof SinkRecord) {
        try {
          offset = ((SinkRecord) record).kafkaOffset();
        } catch (Exception e) {
          log.debug("Failed to extract offset from record, treating as null. Error: {}", e.getMessage());
        }
      }
      auditValue.put("offset", offset);
      
      // Set sync_date: use record timestamp if available, otherwise current time
      // sync_date represents when the record was processed/synced, not the original CDC change time
      // Use Int64 (epoch milliseconds) for ClickHouse DateTime64(3) compatibility with JSON format
      Long syncDateMs = currentTime.getTime();
      if (record.timestamp() != null) {
        try {
          syncDateMs = record.timestamp().longValue();
        } catch (Exception e) {
          log.debug("Failed to extract timestamp from record, using current time for sync_date. Error: {}", e.getMessage());
        }
      }
      auditValue.put("sync_date", syncDateMs);
      
      // Debug logging to verify all fields are extracted
      if (log.isDebugEnabled()) {
        log.debug("Extracted fields - Action: {}, NewValues count: {}, OriginalValues count: {}, FieldIncludes count: {}, PrimaryKeys count: {}", 
            action, afterValues.size(), beforeValues.size(), fieldIncludes.size(), keyValues.size());
        log.debug("NewValues fields: {}", afterValues.keySet());
        log.debug("OriginalValues fields: {}", beforeValues.keySet());
      }
      
      // Warn if NewValues seems incomplete for INSERT
      if ("INSERT".equals(action.toUpperCase()) && afterValues.size() < 5) {
        log.warn("INSERT operation detected but NewValues only contains {} fields. This may indicate missing fields. Fields: {}", 
            afterValues.size(), afterValues.keySet());
      }
      
      return newRecord(record, this.updatedSchema, auditValue);
    } catch (JsonProcessingException ex) {
      log.error("Failed to serialize JSON for CDC record. Topic: {}, Partition: {}", 
          record.topic(), record.kafkaPartition(), ex);
      throw new RuntimeException("Failed to serialize JSON: " + ex.getMessage(), ex);
    } catch (Exception ex) {
      log.error("Error processing CDC record. Topic: {}, Partition: {}", 
          record.topic(), record.kafkaPartition(), ex);
      throw new RuntimeException("Error processing CDC record: " + ex.getMessage(), ex);
    } 
  }
  
  public ConfigDef config() {
    return CONFIG_DEF;
  }
  
  public void close() {
    // Cleanup resources if needed
    this.objectMapper = null;
    this.dateTimeFormatter = null;
    this.updatedSchema = null;
  }
  
  protected abstract Schema operatingSchema(R paramR);
  
  protected abstract Object operatingValue(R paramR);
  
  protected abstract R newRecord(R paramR, Schema paramSchema, Object paramObject);
  
  public static class Key<R extends ConnectRecord<R>> extends ExtractAuditRecordState<R> {
    protected Object operatingValue(R record) {
      return record.key();
    }
    
    protected Schema operatingSchema(R record) {
      return record.keySchema();
    }
    
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return (R)record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
    }
  }
  
  public static class Value<R extends ConnectRecord<R>> extends ExtractAuditRecordState<R> {
    protected Object operatingValue(R record) {
      return record.value();
    }
    
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }
    
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return (R)record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }
  }
  
  private static interface ConfigName {
    public static final String EXCLUDE = "exclude";
    
    public static final String INCLUDE = "include";
    
    public static final String TIMEZONE = "timezone";
    
    public static final String ID = "id";
    
    public static final String TOPIC = "Topic";
    
    public static final String PARTITION = "Partition";
    
    public static final String CHANGETYPE = "ChangeType";
    
    public static final String CHANGETIME = "ChangeTime";
    
    public static final String PRIMARYKEYS = "PrimaryKeys";
    
    public static final String FIELDINCLUDES = "FieldIncludes";
    
    public static final String NEWVALUES = "NewValues";
    
    public static final String ORIGINALVALUES = "OriginalValues";
    
    public static final String DATABASE = "Database";
    
    public static final String TABLE = "Table";
  }
}
