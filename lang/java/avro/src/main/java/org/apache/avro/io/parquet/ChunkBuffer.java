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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.Set;

import org.apache.parquet.format.Encoding;

/**
 * Responsibilities: memory-manage for chunk-buffer, compression,
 * keeping track of compressed vs uncompressed sizes, maintaining
 * chunk-wide information/statistics.
 */
class ChunkBuffer {
  /** TODO: use something like Parquet's CapacityByteArrayOutputStream */
  private ByteArrayOutputStream buf;

  private int pageCount;
  private int valueCount;
  private int compressedDelta;
  public final Set<Encoding> encodings;

  public ChunkBuffer() {
    this.buf = new ByteArrayOutputStream(128*1024);
    encodings = new HashSet<Encoding>(6);
    newChunk();
  }

  public OutputStream asNoncompressingOutputStream() {
    return buf;
  }

  public OutputStream asCompressingOutputStream() {
    return buf;
  }

  /** Writes a block of compressed data. */
  public void writeCompressed(byte[] b, int off, int len) throws IOException {
    int initialSize = buf.size();
    buf.write(b, off, len);
    int compressedSize = buf.size() - initialSize;
    this.compressedDelta += (len - compressedSize);
  }

  public void registerPageInfo(int valueCountDelta, Encoding encoding) {
    this.pageCount++;
    this.valueCount += valueCountDelta;
    this.encodings.add(encoding);
  }

  public void addEncoding(Encoding encoding) {
    this.encodings.add(encoding);
  }

  public int valueCount() { return this.valueCount; }

  public int compressedSize() { return this.buf.size(); }

  public int uncompressedSize() {
    return this.compressedSize() + compressedDelta;
  }

  public Set<Encoding> encodings() { return encodings; }

  public void writeTo(OutputStream out) throws IOException {
    buf.writeTo(out);
  }

  public void newChunk() {
    buf.reset();
    pageCount = 0;
    valueCount = 0;
    compressedDelta = 0;
    encodings.clear();
  }
}

