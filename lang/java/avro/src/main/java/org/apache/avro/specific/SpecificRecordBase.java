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

import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.Decoder;
import org.apache.avro.generic.Advancer;

/** Base class for generated record classes. */
public abstract class SpecificRecordBase
    implements SpecificRecord, Comparable<SpecificRecord>, GenericRecord, Externalizable {

  @Override
  public abstract Schema getSchema();

  @Override
  public abstract Object get(int field);

  @Override
  public abstract void put(int field, Object value);

  public SpecificData getSpecificData() {
    // Default implementation for backwards compatibility, overridden in generated
    // code
    return SpecificData.get();
  }

  public Conversion<?> getConversion(int field) {
    // for backward-compatibility. no older specific classes have conversions.
    return null;
  }

  @Override
  public void put(String fieldName, Object value) {
    put(getSchema().getField(fieldName).pos(), value);
  }

  @Override
  public Object get(String fieldName) {
    return get(getSchema().getField(fieldName).pos());
  }

  public Conversion<?> getConversion(String fieldName) {
    return getConversion(getSchema().getField(fieldName).pos());
  }

  @Override
  public boolean equals(Object that) {
    if (that == this)
      return true; // identical object
    if (!(that instanceof SpecificRecord))
      return false; // not a record
    if (this.getClass() != that.getClass())
      return false; // not same schema
    return getSpecificData().compare(this, that, this.getSchema(), true) == 0;
  }

  @Override
  public int hashCode() {
    return getSpecificData().hashCode(this, this.getSchema());
  }

  @Override
  public int compareTo(SpecificRecord that) {
    return getSpecificData().compare(this, that, this.getSchema());
  }

  @Override
  public String toString() {
    return getSpecificData().toString(this);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    new SpecificDatumWriter(getSchema()).write(this, SpecificData.getEncoder(out));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException {
    new SpecificDatumReader(getSchema()).read(this, SpecificData.getDecoder(in));
  }

  /**
   * Returns true iff an instance supports the {@link #encode} and {@link #decode}
   * operations. Should only be used by <code>SpecificDatumReader/Writer</code> to
   * selectively use {@link #customEncode} and {@link #customDecode} to optimize
   * the (de)serialization of values.
   */
  protected boolean hasCustomCoders() {
    return false;
  }

  protected void customEncode(Encoder out) throws IOException {
    throw new UnsupportedOperationException();
  }

  protected void customDecode(ResolvingDecoder in) throws IOException {
    throw new UnsupportedOperationException();
  }

  /** Fill the fields of <code>this</code> with data from
   * <code>in</code> using advancement records from <code>ra</code>.
   * This method is meant for generated code which might not always be
   * able to handle all cases.  If the current case cannot be handled,
   * then <code>false</code> is returned, and the caller should
   * attempt to read <code>this</code> from <code>in</code> using more
   * generic means.  If <code>this</code> can be read, then
   * <code>true</code> is returned.  An exception is thrown is
   * something goes wrong while attempting the read.
   *
   * <code>ra.reader</code> must, of course, match the schema of
   * <code>this</code>, and <code>ra.writer</code> the schema used to
   * write <code>in</code>.  Unpredictable things will happen if this
   * is not the case.
   */
  public boolean fastRead(Advancer.Record ra, Decoder in) {
    return false;
  }
}
