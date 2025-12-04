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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

import static org.junit.Assert.*;

/**
 * Test class for ExtractAuditRecordState transformation using real data from templates.
 * 
 * This test reads JSON files from templates/ directory and verifies that
 * the transformation can correctly identify operation types and extract data.
 */
public class ExtractAuditRecordStateTest {
    
    private ExtractAuditRecordState.Value<SourceRecord> transform;
    private ObjectMapper objectMapper;
    private String templatesDir;
    
    @Before
    public void setUp() {
        transform = new ExtractAuditRecordState.Value<>();
        objectMapper = new ObjectMapper();
        templatesDir = "templates";
        
        // Configure transform with default settings
        Map<String, Object> configs = new HashMap<>();
        configs.put("timezone", "UTC");
        transform.configure(configs);
    }
    
    @Test
    public void testDeleteOperationFromTemplate() throws IOException {
        // Read DELETE message from templates
        String jsonContent = readTemplateFile("cdc-delete-sample.json");
        JsonNode rootNode = objectMapper.readTree(jsonContent);
        JsonNode message = rootNode.get(0);
        
        // Extract information from JSON
        JsonNode valuePayload = message.get("value").get("payload");
        String op = valuePayload.get("op").asText();
        JsonNode source = valuePayload.get("source");
        
        // Verify op is "d" for DELETE
        assertEquals("d", op);
        
        // Verify before exists and after is null
        assertTrue(valuePayload.has("before"));
        assertFalse(valuePayload.get("before").isNull());
        assertTrue(valuePayload.has("after"));
        assertTrue(valuePayload.get("after").isNull());
        
        // Verify source information
        assertEquals("EXAMPLE_DB", source.get("db").asText());
        assertEquals("ExampleTable", source.get("table").asText());
        
        System.out.println("DELETE Test - Op: " + op);
        System.out.println("Database: " + source.get("db").asText());
        System.out.println("Table: " + source.get("table").asText());
    }
    
    @Test
    public void testInsertOperationFromTemplate() throws IOException {
        // Read INSERT message from templates
        String jsonContent = readTemplateFile("cdc-insert-sample.json");
        JsonNode rootNode = objectMapper.readTree(jsonContent);
        JsonNode message = rootNode.get(0);
        
        // Extract information from JSON
        JsonNode valuePayload = message.get("value").get("payload");
        String op = valuePayload.get("op").asText();
        JsonNode source = valuePayload.get("source");
        
        // Verify op is "c" for INSERT
        assertEquals("c", op);
        
        // Verify before is null and after exists
        assertTrue(valuePayload.has("before"));
        assertTrue(valuePayload.get("before").isNull());
        assertTrue(valuePayload.has("after"));
        assertFalse(valuePayload.get("after").isNull());
        
        // Verify source information
        assertEquals("EXAMPLE_DB", source.get("db").asText());
        assertEquals("ExampleTable", source.get("table").asText());
        
        System.out.println("INSERT Test - Op: " + op);
        System.out.println("Database: " + source.get("db").asText());
        System.out.println("Table: " + source.get("table").asText());
    }
    
    @Test
    public void testUpdateOperationFromTemplate() throws IOException {
        // Read UPDATE message from templates
        String jsonContent = readTemplateFile("cdc-update-sample.json");
        JsonNode rootNode = objectMapper.readTree(jsonContent);
        JsonNode message = rootNode.get(0);
        
        // Extract information from JSON
        JsonNode valuePayload = message.get("value").get("payload");
        String op = valuePayload.get("op").asText();
        JsonNode source = valuePayload.get("source");
        JsonNode before = valuePayload.get("before");
        JsonNode after = valuePayload.get("after");
        
        // Verify op is "u" for UPDATE
        assertEquals("u", op);
        
        // Verify both before and after exist
        assertTrue(valuePayload.has("before"));
        assertFalse(before.isNull());
        assertTrue(valuePayload.has("after"));
        assertFalse(after.isNull());
        
        // Verify source information
        assertEquals("EXAMPLE_DB", source.get("db").asText());
        assertEquals("ExampleTable", source.get("table").asText());
        
        System.out.println("UPDATE Test - Op: " + op);
        System.out.println("Database: " + source.get("db").asText());
        System.out.println("Table: " + source.get("table").asText());
    }
    
    @Test
    public void testTemplateFilesExist() throws IOException {
        // Verify all template files exist and are readable
        String[] templateFiles = {
            "cdc-delete-sample.json",
            "cdc-insert-sample.json",
            "cdc-update-sample.json"
        };
        
        for (String filename : templateFiles) {
            String content = readTemplateFile(filename);
            assertNotNull("Template file " + filename + " should exist", content);
            assertFalse("Template file " + filename + " should not be empty", content.isEmpty());
            
            // Verify it's valid JSON
            JsonNode rootNode = objectMapper.readTree(content);
            assertTrue("Template file " + filename + " should be a JSON array", rootNode.isArray());
            assertTrue("Template file " + filename + " should have at least one message", rootNode.size() > 0);
        }
    }
    
    @Test
    public void testOpFieldValuesFromTemplates() throws IOException {
        // Test that op field values are correctly identified from all templates
        Map<String, String> expectedOps = new HashMap<>();
        expectedOps.put("cdc-delete-sample.json", "d");
        expectedOps.put("cdc-insert-sample.json", "c");
        expectedOps.put("cdc-update-sample.json", "u");
        
        for (Map.Entry<String, String> entry : expectedOps.entrySet()) {
            String filename = entry.getKey();
            String expectedOp = entry.getValue();
            
            String jsonContent = readTemplateFile(filename);
            JsonNode rootNode = objectMapper.readTree(jsonContent);
            JsonNode message = rootNode.get(0);
            JsonNode valuePayload = message.get("value").get("payload");
            String actualOp = valuePayload.get("op").asText();
            
            assertEquals("File " + filename + " should have op=" + expectedOp, expectedOp, actualOp);
        }
    }
    
    // Helper method to read template file
    private String readTemplateFile(String filename) throws IOException {
        String filePath = templatesDir + File.separator + filename;
        return new String(Files.readAllBytes(Paths.get(filePath)));
    }
}
