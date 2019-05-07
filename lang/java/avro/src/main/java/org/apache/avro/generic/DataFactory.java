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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.Advancer;
import org.apache.avro.io.Decoder;
import org.apache.avro.specific.SpecificData;


/**
 * Factory for creating objects while decoding an Avro input stream.
 *
 * @see Reader
 */
public class DataFactory {

  /**
   * This class is a cleaned-up version of {@link GenericData} and
   * {@link SpecificData}.  Delegate to {@link SpecificData} when the
   * two classes have functionality that overlaps.
   */
  private final SpecificData data;

  /** For subclasses. Applications call {@link DataFactory#getDefault()}. */
  protected DataFactory(SpecificData data) {
    this.data = data;
  }

  /** Return the singleton instance. */
  public static DataFactory getDefault(Advancer a) {
    return new DataFactory(SpecificData.getForSchema(a.reader));
  }

  /**
   * Called to create an fixed value. May be overridden for alternate fixed
   * representations. By default, returns {@link GenericFixed}.
   */
  public GenericFixed newFixed(Schema schema) {
    return (GenericFixed) data.createFixed(null, schema);
  }

  /**
   * Called to create an enum value. May be overridden for alternate enum
   * representations. By default, returns a GenericEnumSymbol.
   */
  public Object newEnum(String symbol, Schema schema) {
    return data.createEnum(symbol, schema);
  }

  /**
   * Called to create new record instances. Subclasses may override to use a
   * different record implementation. The returned instance must conform to the
   * schema provided. If the old object contains fields not present in the schema,
   * they should either be removed from the old object, or it should create a new
   * instance that conforms to the schema. By default, this returns a
   * {@link GenericData.Record}.
   */
  public IndexedRecord newRecord(Object old, Schema schema) {
    return (IndexedRecord) data.newRecord(old, schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different array implementation. By default, this returns a
   * {@link GenericData.Array}.
   */
  public Collection newArray(Object old, int size, Schema schema) {
    return data.newArray(old, size, schema);
  }

  /**
   * Called to create new array instances. Subclasses may override to use a
   * different map implementation. By default, returns a Map from java.util.
   */
  public Map newMap(Object old, int size) {
    return data.newMap(old, size);
  }
}
