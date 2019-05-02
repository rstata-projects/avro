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
package org.apache.avro.specific;

import java.io.IOException;

import org.apache.avro.Resolver;
import org.apache.avro.Schema;
import org.apache.avro.generic.Advancer;
import org.apache.avro.generic.GenericDatumReader2;
import org.apache.avro.io.Decoder;

public class SpecificDatumReader2<D> extends GenericDatumReader2<D> {
  private final Advancer.Record recordAdvancer;

  private SpecificDatumReader2(Advancer a, SpecificData d) {
    super(a, d);
    if (a.reader.getType() == Schema.Type.RECORD)
      this.recordAdvancer = (Advancer.Record) a;
    else
      this.recordAdvancer = null;
  }

  /**
   * ... Document how we use <code>d:</code> to create fixed, array, map, and
   * record objects.
   */
  public static SpecificDatumReader2 getReaderFor(Schema w, Schema r, SpecificData d) {
    // TODO: add caching
    Resolver.Action a = Resolver.resolve(w, r, d);
    Advancer adv = Advancer.from(a);
    return new SpecificDatumReader2(adv, d);
  }

  /**
   * Read a specific record with minimal overhead. Specific records with compiled
   * code are typically used when large numbers of records are being read out of a
   * file and processed in some inner loop. This method is designed to support
   * that case. To this end, the advancer object used to initialize
   * <code>this</code> must be for a record type, which in turn must be the record
   * type of <code>reuse</code> (a subclass of {@link SpecificRecordBase}), an
   * <code>reuse</code> cannot be null. A null-pointer or class-cast exception
   * will be thrown if all of this is not true.
   */
  public D readRecord(D reuse, Decoder in) throws IOException {
    SpecificRecordBase o = (SpecificRecordBase) reuse;
    if (o.fastRead(recordAdvancer, in))
      return (D) o;
    return (D) readRecord(reuse, recordAdvancer, in);
  }
}
