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
 * Responsibilities: memory-management for page buffer, encoding of
 * values, maintaining page-level info/statistics, formatting Data
 * Page (v2) and Dictionary Pages (writing them into ChunkBuffers).
 */
abstract class ValueBuffer {
  public void putInt(int i) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void putLong(long l) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void putFloat(float f) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void putDouble(double d) throws IOException {
    throw new UnsupportedOperationException();
  }

  public void putBytes(byte[] b, int start, int len) throws IOException {
    throw new UnsupportedOperationException();
  }

  public abstract int valueCount() throws IOException;

  public abstract int byteCount() throws IOException;

  public abstract Parquet.Encoding encoding() throws IOException;

  public abstract void newPage() throws IOException;

  public boolean hasDictionary() { return false; }

  public abstract void writeDataTo(OutputStream cb) throws IOException;

  public abstract void writeDictTo(OutputStream cb) throws IOException;

  public void newChunk() throws IOException { }

  public static ValueBuffer get(Column c) {
    switch (c.encoding) {
    case PLAIN:
      return PlainValueBuffer.get(c);

    default:
      throw new IllegalArgumentException("Upsupported encoding: "+c.encoding);
    }
  }
}
