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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.io.api.Binary;

/**
 * Responsibilities: memory-management for page buffer (delegated to
 * ValuesBuffer), encoding of values (also delegated to ValuesBuffer),
 * maintaining page-level info/statistics (mainly delegated to
 * ValuesBuffer), formatting Data Page (v2) and Dictionary Pages
 * (writing them into ChunkBuffers).
 */
class PageBuffer extends Parquet.ColumnWriter {
  private final ValuesWriter data;
  /* TODO: Eventually, we'll need to keep track of three streams of
   * data: the actual data values, plus the rep- and def-levels
   * information.  Each of them will be represented by its own
   * ValueBuffer. */

  private int valueCount;

  private PageBuffer(ValuesWriter data) {
    this.data = data;
    this.valueCount = 0;
  }

  public void putInt(int i) throws IOException {
    data.writeInteger(i);
    valueCount++;
  }

  public void putLong(long l) throws IOException {
    data.writeLong(l);
    valueCount++;
  }

  public void putFloat(float f) throws IOException {
    data.writeFloat(f);
    valueCount++;
  }

  public void putDouble(double d) throws IOException {
    data.writeDouble(d);
    valueCount++;
  }

  public void putBytes(byte[] b, int start, int len) throws IOException {
    data.writeBytes(Binary.fromReusedByteArray(b, start, len));
    valueCount++;
  }

  /** Create a DataPage (v2) entry in <code>cb</code> for values
   * in <code>this</code>. */
  public void writeDataPageTo(ChunkBuffer cb) throws IOException {
    int byteCount = (int)data.getBufferedSize();
    Encoding encoding = PMC.getEncoding(data.getEncoding());

    Formatting.DataPageInfo dpi =
      new Formatting.DataPageInfo(encoding,
                                  valueCount, 0, valueCount,
                                  byteCount, byteCount,
                                  0, 0);

    dpi.writeTo(cb.asNoncompressingOutputStream());
    /* TODO: insert rep- and def-level data here. */
    data.getBytes().writeAllTo(cb.asCompressingOutputStream());
    cb.registerPageInfo(valueCount, encoding);
  }

  private static final ParquetMetadataConverter PMC
    = new ParquetMetadataConverter();

  public int writeDictPageTo(ChunkBuffer cb) throws IOException {
    // TODO -- how does compression work for dictionary pages???
    OutputStream out = cb.asCompressingOutputStream();
    DictionaryPage dict = data.toDictPageAndClose();
    if (dict == null) return 0;
    CountingOutputStream o = new CountingOutputStream(out);
    PMC.writeDictionaryPageHeader(
      dict.getUncompressedSize(),
      (int)dict.getBytes().size(),
      dict.getDictionarySize(),
      dict.getEncoding(),
      o);
    dict.getBytes().writeAllTo(o);
    cb.addEncoding(PMC.getEncoding(dict.getEncoding()));
    return (int)o.getCount();
  }

  public void newPage() throws IOException {
    data.reset();
    valueCount = 0;
  }

  public void newChunk() throws IOException {
    newPage();
  }

  public static PageBuffer get(Formatting.ColumnInfo ci) {
    ColumnDescriptor cd
      = new ColumnDescriptor(ci.pathAsArray, ci.typeName, -1, 0);
    return new PageBuffer(WVB.newValuesWriter(cd));
  }

  private static final ParquetProperties WVB
    = ParquetProperties.builder().build();
}
