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

  public void add(Column c) {
    this.cols.add(c);
    this.colInfos.add(c.info);
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


  static public abstract class Column {
    public final String name;
    public final Type type;
    public final OriginalType originalType;
    public final Encoding encoding;
    public final Integer len;
    protected final Formatting.ColumnInfo info;
    protected final PageBuffer pb;
    protected final ChunkBuffer cb;

    protected Column(String n, Type t, OriginalType ot,
                     Encoding e, Integer len)
    {
      this.name = n;
      this.type = t;
      this.originalType = ot;
      this.encoding = e;
      this.len = len;
      this.pb = PageBuffer.get(this);
      this.cb = new ChunkBuffer();
      this.info = new Formatting.ColumnInfo(name, t, ot, e, len);
    }

    public void flushPage() throws IOException {
      pb.writeDataPageTo(cb);
      pb.newPage();
    }

    public Formatting.ChunkInfo writeChunk(OutputStream out, long chunkOffset)
      throws IOException
    {
      flushPage();
      int dictLen = 0;
      if (pb.hasDictionary()) {
        dictLen = pb.writeDictPageTo(out);
      }
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

    public static class Int extends Column {
      public Int(String name, OriginalType ot, Encoding e) {
        super(name, Type.INT32, ot, e, null);
      }

      public void write(int i) throws IOException {
        this.pb.putInt(i);
      }
    }

    public static class Long extends Column {
      public Long(String name, OriginalType ot, Encoding e) {
        super(name, Type.INT64, ot, e, null);
      }

      public void write(long l) throws IOException {
        this.pb.putLong(l);
      }
    }

    public static class Float extends Column {
      public Float(String name, OriginalType ot, Encoding e) {
        super(name, Type.FLOAT, ot, e, null);
      }

      public void write(float f) throws IOException {
        this.pb.putFloat(f);
      }
    }

    public static class Double extends Column {
      public Double(String name, OriginalType ot, Encoding e) {
        super(name, Type.DOUBLE, ot, e, null);
      }

      public void write(double d) throws IOException {
        this.pb.putDouble(d);
      }
    }

    public static class Bytes extends Column {
      public Bytes(String name, OriginalType ot, Encoding e) {
        super(name, Type.BYTE_ARRAY, ot, e, null);
      }

      public void write(byte[] b, int start, int len) throws IOException {
        this.pb.putBytes(b, start, len);
      }
    }

    public static class FixedBytes extends Column {
      public final int len;
      public FixedBytes(String name, OriginalType ot, Encoding e, int len) {
        super(name, Type.FIXED_LENGTH_BYTE_ARRAY, ot, e, len);
        this.len = len;
      }
      public void write(byte[] b, int start) throws IOException {
        this.pb.putBytes(b, start, len);
      }
    }
  }
}
