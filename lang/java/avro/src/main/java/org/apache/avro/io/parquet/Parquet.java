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

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * The class that generates validating grammar.
 */
public class Parquet implements Closeable {
  public enum Type {
    BOOLEAN, INT32, INT64, INT96, FLOAT, DOUBLE, BYTE_ARRAY,
    FIXED_LENGTH_BYTE_ARRAY,
  };

  public enum OriginalType { UTF8 };

  public enum Encoding {
    PLAIN, UNUSED, PLAIN_DICTIONARY, RLE, BIT_PACKED,
    DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY,
    RLE_DICTIONARY,
  };

  public enum CompressionCodec {
      UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI,
  };


  private OutputStream out;
  private int bytesWritten;
  private List<Column> cols;
  private List<Formatting.ColumnInfo> colInfos;
  private List<Formatting.RowInfo> rowGroups;
  private int rowsPerGroup;
  private int rowsThisGroup;
  private int rowsForNextSizeCheck;


  public Parquet(OutputStream o, int rowsPerGroup) throws IOException {
    Formatting.magicNumber(out);
    this.out = out;
    this.bytesWritten = 0;
    this.cols = new ArrayList<Column>(10);
    this.colInfos = new ArrayList<Formatting.ColumnInfo>(10);
    this.rowsPerGroup = rowsPerGroup;
    this.rowsThisGroup = 0;
    this.rowsForNextSizeCheck = Math.min(100, rowsPerGroup);
  }

  private ColumnWriter add(Column c) {
    this.cols.add(c);
    this.colInfos.add(c.info);
    return c.getColumnWriter();
  }

  public ColumnWriter addIntColumn(String n, Encoding e) {
    return add(new Column(n, Type.INT32, null, e, 0));
  }

  public ColumnWriter addLongColumn(String n, Encoding e) {
    return add(new Column(n, Type.INT64, null, e, 0));
  }

  public ColumnWriter addFloatColumn(String n, Encoding e) {
    return add(new Column(n, Type.FLOAT, null, e, 0));
  }

  public ColumnWriter addDoubleColumn(String n, Encoding e) {
    return add(new Column(n, Type.DOUBLE, null, e, 0));
  }

  public ColumnWriter addStringColumn(String n, Encoding e) {
    return add(new Column(n, Type.BYTE_ARRAY, OriginalType.UTF8, e, 0));
  }

  public ColumnWriter addBytesColumn(String n, Encoding e) {
    return add(new Column(n, Type.BYTE_ARRAY, null, e, 0));
  }

  public ColumnWriter addFixedBytesColumn(String n, Encoding e, int len) {
    return add(new Column(n, Type.FIXED_LENGTH_BYTE_ARRAY, null, e, len));
  }

  public void endRow() throws IOException {
    rowsThisGroup++;
    if (rowsForNextSizeCheck <= rowsThisGroup) {
      if (rowsPerGroup <= rowsThisGroup) {
        endRowGroup();
      } else {
        flushFullPages();
      }
    }
  }

  public void flushFullPages() throws IOException {
    for (Column c: cols) {
      // TODO: flush only when getting full
      c.flushPage();
    }
    rowsForNextSizeCheck = Math.min(rowsPerGroup, 100+rowsForNextSizeCheck);
  }

  public void endRowGroup() throws IOException {
    if (0 < this.rowsThisGroup) {
      List<Formatting.ChunkInfo> chunks
        = new ArrayList<Formatting.ChunkInfo>(cols.size());
      for (Column c: cols) {
        Formatting.ChunkInfo chunk = c.writeChunk(out, bytesWritten);
        bytesWritten += chunk.compressedSize;
        chunks.add(chunk);
      }
      this.rowGroups.add(new Formatting.RowInfo(this.rowsThisGroup, chunks));
      this.rowsThisGroup = 0;
      this.rowsForNextSizeCheck = Math.min(100, rowsPerGroup);
    }
  }

  public void close() throws IOException {
    endRowGroup();
    Formatting.writeFooter(out, bytesWritten, colInfos, rowGroups);
    out.close();
  }

  public static abstract class ColumnWriter {
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
  }
}
