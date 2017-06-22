/**
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
package org.apache.avro.io.parquet;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Responsibilities: memory-management for page buffer (delegated to
 * ValuesBuffer), encoding of values (also delegated to ValuesBuffer),
 * maintaining page-level info/statistics (mainly delegated to
 * ValuesBuffer), formatting Data Page (v2) and Dictionary Pages
 * (writing them into ChunkBuffers).
 */
class PageBuffer extends Parquet.ColumnWriter {
  private final ValueBuffer data;
  /* TODO: Eventually, we'll need to keep track of three streams of
   * data: the actual data values, plus the rep- and def-levels
   * information.  Each of them will be represented by its own
   * ValueBuffer. */

  private PageBuffer(ValueBuffer data) {
    this.data = data;
  }

  public void putInt(int i) throws IOException {
    data.putInt(i);
  }

  public void putLong(long l) throws IOException {
    data.putLong(l);
  }

  public void putFloat(float f) throws IOException {
    data.putFloat(f);
  }

  public void putDouble(double d) throws IOException {
    data.putDouble(d);
  }

  public void putBytes(byte[] b, int start, int len) throws IOException {
    data.putBytes(b, start, len);
  }

  /** Create a DataPage (v2) entry in <code>cb</code> for values
   * in <code>this</code>. */
  public void writeDataPageTo(ChunkBuffer cb) throws IOException {
    int valueCount = data.valueCount();
    int byteCount = data.byteCount();
    Parquet.Encoding encoding = data.encoding();

    Formatting.DataPageInfo dpi =
      new Formatting.DataPageInfo(encoding,
                                  valueCount, 0, valueCount,
                                  byteCount, byteCount,
                                  0, 0);

    dpi.writeTo(cb.asNoncompressingOutputStream());
    /* TODO: insert rep- and def-level data here. */
    data.writeDataTo(cb.asCompressingOutputStream());
    cb.registerPageInfo(valueCount, encoding);
  }

  public int writeDictPageTo(OutputStream out) throws IOException {
    throw new UnsupportedOperationException("Not implemented yet.");
  }

  public boolean hasDictionary() { return data.hasDictionary(); }

  public void newPage() throws IOException { data.newPage(); }

  public void newChunk() throws IOException { data.newChunk(); }

  public static PageBuffer get(Column c) {
    switch (c.encoding) {
    case PLAIN:
      return new PageBuffer(PlainValueBuffer.get(c));

    default:
      throw new IllegalArgumentException("Upsupported encoding: "+c.encoding);
    }
  }
}
