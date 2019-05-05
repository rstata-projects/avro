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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.io.Decoder;

public class Reader {

  public static <D> D read(D reuse, Advancer a, Decoder in) throws IOException {
    Object result = read0(reuse, a, DataFactory.getDefault(), in);
    LogicalType logicalType = a.reader.getLogicalType();
    if (logicalType != null) {
      Conversion<?> conversion = data.getConversionFor(logicalType);
      if (conversion != null) {
        result= convert(result, a.reader, logicalType, conversion);
      }
    }
  }

  public static <D> D read(D reuse, Advancer a, DataFactory df, Decoder in)
    throws IOException
  {
    return (D) read0(reuse, a, df, in);
    LogicalType logicalType = a.reader.getLogicalType();
    if (logicalType != null) {
      Conversion<?> conversion = data.getConversionFor(logicalType);
      if (conversion != null) {
        result= convert(result, a.reader, logicalType, conversion);
      }
    }
  }

  private static Object read0(Object reuse, Advancer a, DataFactory df, Decoder in)
    throws IOException
  {
    switch (a.reader.getType()) {
    case NULL:
      return a.nextNull(in);

    case BOOLEAN:
      return (Boolean) a.nextBoolean(in);

    case INT:
      return (Integer) a.nextInt(in);

    case LONG:
      return (Long) a.nextLong(in);

    case FLOAT:
      return (Float) a.nextFloat(in);

    case DOUBLE:
      return (Double) a.nextDouble(in);

    case STRING:
      return a.nextString(in);

    case BYTES:
      return a.nextBytes(in, (ByteBuffer) reuse);

    case ENUM:
      return df.newEnum(a.reader.getEnumSymbols().get(in.readEnum()), a.reader);

    case FIXED: {
      int sz = a.reader.getFixedSize();
      byte[] bytes;
      if (reuse instanceof GenericFixed
          && (bytes = ((GenericFixed) reuse).bytes()).length == sz)
        ;
      else {
        bytes = new byte[sz];
        reuse = df.newFixed(bytes, a.reader);
      }
      a.nextFixed(in, bytes);
      return reuse;
    }

    case UNION:
      return read0(reuse, advancer.getBranchAdvancer(in, advancer.nextIndex(in)), df, in);

    case ARRAY: {
      Advancer.Array c = (Advancer.Array) advancer;
      Advancer ec = c.elementAdvancer;
      long i = c.firstChunk(in);
      reuse = df.newArray(reuse, (int) i, a.reader);
      GenericData.Array sda = (reuse instanceof GenericData.Array ? (GenericData.Array) reuse : null);

      Collection array = (Collection) reuse;
      for (; i != 0; i = c.nextChunk(in))
        for (; i != 0; i--) {
          Object v = read0(sda != null ? sda.peek() : null, ec, df, in);
          // TODO -- logical type conversion
          array.add(v);
        }
      return array;
    }

    case MAP: {
      Advancer.Map c = (Advancer.Map) advancer;
      Advancer kc = c.keyAdvancer;
      Advancer vc = c.valAdvancer;
      long i = c.firstChunk(in);
      Map map = (Map) df.newMap(reuse, (int) i);
      for (; i != 0; i = c.nextChunk(in))
        for (; i != 0; i--) {
          Object key = kc.nextString(in);
          Object val = read0(null, vc, df, in);
          map.put(key, val);
        }
      return map;
    }

    case RECORD: {
      Advancer.Record ra = (Advancer.Record) a;
      IndexRecord o = df.newRecord(reuse, ra.reader);
      if (reuse instanceof SpecificRecordBase)
        if (((SpecificRecordBase) reuse).fastRead0(ra, df, in))
          return reuse;
      for (int i = 0; i < a.advancers.length; i++) {
        int p = a.readerOrder[i].pos();
        o.put(p, read0(null, a.advancers[i], df, in));
      }
      a.done(in);
      return o;
    }

    default:
      throw new IllegalArgumentException("Can't handle this yet.");
    }
  }
}
