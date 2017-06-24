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
 * A "writer" class for Parquet columns.  Similar to Parquet's
 * ColumnWriter* classes.
 */
class Column {
  protected final PageBuffer pb;
  protected final ChunkBuffer cb;

  public Column(Formatting.ColumnInfo ci) {
    this.pb = PageBuffer.get(ci);
    this.cb = new ChunkBuffer();
  }

  public Parquet.ColumnWriter getColumnWriter() {
    return pb;
  }

  public int sizeCheck(int rowsThisGroup) throws IOException {
    // TODO: implement logic to determine if flushes are needed and
    // also how many rows to write until checking again
    flushPage();
    return Integer.MAX_VALUE;
  }

  private void flushPage() throws IOException {
    pb.writeDataPageTo(cb);
    pb.newPage();
  }

  public Formatting.ChunkInfo writeChunk(OutputStream out, long chunkOffset)
    throws IOException
  {
    flushPage();
    int dictLen = 0;
    dictLen = pb.writeDictPageTo(cb);
    cb.writeTo(out);

    Formatting.ChunkInfo result =
        new Formatting.ChunkInfo(chunkOffset, chunkOffset+dictLen,
                                 cb.valueCount(),
                                 cb.uncompressedSize(), cb.compressedSize(),
                                 cb.encodings());
    pb.newChunk();
    cb.newChunk();
    return result;
  }
}
