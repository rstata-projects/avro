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

import org.apache.avro.Conversions;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.avro.io.Decoder;

public class Reader {

  public static <D> D read(D reuse, Advancer a, Decoder in) throws IOException {
    return (D) read(reuse, a, DataFactory.getDefault(a), in);
  }

  public static <D> D read(D reuse, Advancer a, DataFactory df, Decoder in)
    throws IOException
  {
    Object result = read0(reuse, a, df, in);
    if (a.conversion != null)
      result = Conversions.convertToLogicalType(result, a.reader,
                                                a.logicalType, a.conversion);
    return (D) result;
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

    case FIXED:
      int sz = a.reader.getFixedSize();
      byte[] bytes;
      if (reuse instanceof GenericFixed
          && (bytes = ((GenericFixed) reuse).bytes()).length == sz)
        ;
      else {
        GenericFixed o = df.newFixed(a.reader);
        reuse = o;
        bytes = new byte[sz];
      }
      a.nextFixed(in, bytes);
      return reuse;

    case UNION:
      return read(reuse, a.getBranchAdvancer(in, a.nextIndex(in)), df, in);

    case ARRAY: {
      Advancer.Array c = (Advancer.Array) a;
      Advancer ec = c.elementAdvancer;
      long i = c.firstChunk(in);
      Collection sda = df.newArray(reuse, (int) i, a.reader);
      if (sda instanceof GenericArray) {
        GenericArray ga = (GenericArray) sda;
        for (; i != 0; i = c.nextChunk(in))
          for (; i != 0; i--)
            ga.add(read(ga.peek(), ec, df, in));
      } else
        for (; i != 0; i = c.nextChunk(in))
          for (; i != 0; i--)
            sda.add(read(null, ec, df, in));
      return sda;
    }

    case MAP: {
      Advancer.Map c = (Advancer.Map) a;
      Advancer kc = c.keyAdvancer;
      Advancer vc = c.valAdvancer;
      long i = c.firstChunk(in);
      Map map = (Map) df.newMap(reuse, (int) i);
      for (; i != 0; i = c.nextChunk(in))
        for (; i != 0; i--) {
          Object key = kc.nextString(in);
          Object val = read(null, vc, df, in);
          map.put(key, val);
        }
      return map;
    }

    case RECORD: {
      Advancer.Record ra = (Advancer.Record) a;
      if (reuse instanceof SpecificRecordBase)
        if (((SpecificRecordBase) reuse).fastRead(ra, df, in))
          return reuse;
      IndexedRecord o = df.newRecord(reuse, ra.reader);
      for (int i = 0; i < ra.advancers.length; i++) {
        int p = ra.readerOrder[i].pos();
        o.put(p, read(null, ra.advancers[i], df, in));
      }
      ra.done(in);
      return o;
    }

    default:
      throw new IllegalArgumentException("Can't handle this yet.");
    }
  }
}
