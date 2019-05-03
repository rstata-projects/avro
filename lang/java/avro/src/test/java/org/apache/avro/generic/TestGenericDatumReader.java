/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.generic;

import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

@RunWith(Parameterized.class)
public class TestGenericDatumReader {
  private final String description;
  private final Schema writerSchema;
  private final Schema readerSchema;
  private final Object input;
  private final Object expectedOutput;

  public TestGenericDatumReader(String description, Schema writerSchema, Schema readerSchema, Object input,
      Object expectedOutput) {
    this.description = description;
    this.writerSchema = writerSchema;
    this.readerSchema = readerSchema;
    this.input = input;
    this.expectedOutput = expectedOutput;
  }

  @Test
  public void testResolution() throws Exception {
    try {
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      Encoder e = EncoderFactory.get().binaryEncoder(os, null);

      DatumWriter<Object> w = new GenericDatumWriter<>(writerSchema);
      w.write(input, e);
      e.flush();

      ByteArrayInputStream is = new ByteArrayInputStream(os.toByteArray());
      Decoder d = DecoderFactory.get().binaryDecoder(is, null);
      DatumReader<Object> r = GenericDatumReader2.getReaderFor(writerSchema, readerSchema, GENERIC_DATA);
      Object output = r.read(null, d);
      Assert.assertEquals(description, expectedOutput, output);
    } catch (Exception e) {
      throw new Exception("Error in \"" + description + "\"", e);
    }
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data2() throws IOException {
    List<Object[]> result = new ArrayList<>(testData.length);
    for (Object[] row : testData) {
      Schema wp = new Schema.Parser().parse((String) (row[1]));
      Schema rp = new Schema.Parser().parse((String) (row[2]));
      result.add(new Object[] { row[0], wp, rp, row[3], row[4] });
    }
    return result;
  }

  private static Object[][] testData;
  private static GenericData GENERIC_DATA = new GenericData();

  static {
    testData = new Object[][] {
        // Identical reader and writer schemas
        { "plain null", "{\"type\": \"null\"}", "{\"type\": \"null\"}", null, null },
        { "plain boolean", "{\"type\": \"boolean\"}", "{\"type\": \"boolean\"}", true, true },
        { "plain int", "{\"type\": \"int\"}", "{\"type\": \"int\"}", 10, 10 },
        { "plain long", "{\"type\": \"long\"}", "{\"type\": \"long\"}", 100L, 100L },
        { "plain float", "{\"type\": \"float\"}", "{\"type\": \"float\"}", 1000.0f, 1000.0f },
        { "plain double", "{\"type\": \"double\"}", "{\"type\": \"double\"}", 10000.0, 10000.0 },
        { "plain string", "{\"type\": \"string\"}", "{\"type\": \"string\"}", "\"s\"", "\"s\"" },
        { "plain bytes", "{\"type\": \"bytes\"}", "{\"type\": \"bytes\"}", ByteBuffer.wrap(new byte[] { 0, 2, 4, 6 }),
            ByteBuffer.wrap(new byte[] { 0, 2, 4, 6 }) },
        { "plain fixed(4)", "{\"name\": \"fxd\", \"type\": \"fixed\", \"size\": 4}",
            "{\"name\": \"fxd\", \"type\": \"fixed\", \"size\": 4}",
            GENERIC_DATA.createFixed(null, new byte[] { 0, 3, 6, 9 }, Schema.createFixed("fxd", "", "", 4)),
            GENERIC_DATA.createFixed(null, new byte[] { 0, 3, 6, 9 }, Schema.createFixed("fxd", "", "", 4)) },
        { "plain union<int, string>", "[\"int\", \"string\"]", "[\"int\", \"string\"]", "\"s\"", "\"s\"" },
        { "plain array<int>", "{\"type\": \"array\", \"items\": \"int\"}", "{\"type\": \"array\", \"items\": \"int\"}",
            Arrays.asList(10, 20, 30), Arrays.asList(10, 20, 30) },
        { "plain map<string, int>", "{\"type\": \"map\", \"values\": \"int\"}",
            "{\"type\": \"map\", \"values\": \"int\"}", new HashMap<String, Integer>() {
              {
                put("a", 1);
                put("b", 2);
              }
            }, new HashMap<String, Integer>() {
              {
                put("a", 1);
                put("b", 2);
              }
            } },
        { "plain enum(a, b)", "{\"name\": \"enm\", \"type\": \"enum\", \"symbols\": [\"A\", \"B\"]}",
            "{\"name\": \"enm\", \"type\": \"enum\", \"symbols\": [\"A\", \"B\"]}",
            GENERIC_DATA.createEnum("A", Schema.createEnum("enm", "", null, Arrays.asList("A", "B"))),
            GENERIC_DATA.createEnum("A", Schema.createEnum("enm", "", null, Arrays.asList("A", "B"))) },
        // {"plain record", "{\"type"\"}"}

        // Nested types

        // Promotions
        { "int to long", "{\"type\": \"int\"}", "{\"type\": \"long\"}", 100, 100L },
        { "int to float", "{\"type\": \"int\"}", "{\"type\": \"float\"}", 101, 101.0f },
        { "int to double", "{\"type\": \"int\"}", "{\"type\": \"double\"}", 102, 102.0 },
        { "long to float", "{\"type\": \"long\"}", "{\"type\": \"float\"}", 103L, 103.0f },
        { "long to double", "{\"type\": \"long\"}", "{\"type\": \"double\"}", 104L, 104.0 },
        { "float to double", "{\"type\": \"float\"}", "{\"type\": \"double\"}", 105.f, 105.0 },
        { "string to bytes", "{\"type\": \"string\"}", "{\"type\": \"bytes\"}", "a10",
            ByteBuffer.wrap(new byte[] { 'a', '1', '0' }) },
        { "bytes to string", "{\"type\": \"bytes\"}", "{\"type\": \"string\"}",
            ByteBuffer.wrap(new byte[] { 'a', '1', '0' }), "a10" },

        // Promotion in array

        // Promotion in map

        // Promotion in record

        { "union<int, string> to string", "[\"int\", \"string\"]", "{\"type\": \"string\"}", "s", "s" },
        { "string to union<int, string>", "{\"type\": \"string\"}", "[\"int\", \"string\"]", "s", "s" },

        { "union<int, string> to union<string, int>", "[\"int\", \"string\"]", "[\"int\", \"string\"]", "\"s\"",
            "\"s\"" },
        { "union<int, string> to union<string, long>", "[\"int\", \"string\"]", "[\"int\", \"string\"]", "\"s\"",
            "\"s\"" },
        // FIXME: some trouble with arrayIndex
        { "union<int, string> to union<long, string, boolean>", "[\"int\", \"string\"]",
            "[\"long\", \"string\", \"boolean\"]", "s", "s" },
        { "union<long, string, boolean> to union<int, string>", "[\"long\", \"string\", \"boolean\"]",
            "[\"int\", \"string\"]", "s", "s" },
        // union cases + promotion
        { "union<int, string> to union<string, long>", "[\"int\", \"string\"]", "[\"int\", \"string\"]", 11, 11L }, };
  }
}
