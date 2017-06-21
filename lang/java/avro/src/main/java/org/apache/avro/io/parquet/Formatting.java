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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// General-purpose
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.FieldRepetitionType;
import org.apache.parquet.format.SchemaElement;
import org.apache.parquet.format.Type;
import org.apache.parquet.format.Util;

// Related to Data Page headers
import org.apache.parquet.format.DataPageHeaderV2;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.PageType;

// Related to File and Row-Group metadata
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.RowGroup;

/**
 * The class handles encoding of the non-column data to a Parquet
 * file.
 */
class Formatting {
  private static byte[] MAGIC_NUMBER = { 'P', 'A', 'R', '1' };

  /** For version field of file metadata.  Value taken from
   * org/apache/parquet/hadoop/ParquetFileWriter */
  private static int VERSION = 1;

  private static String CREATED_BY
    = "avro version 0.8 (build 6cf94d29b2b7115df4de2c06e2ab4326d721eb55)";

  /** Convert from the Parquet.Encoding enumeration to the
    * thrift-generated representation of Parquet's encodings. */
  public static Encoding getEncoding(Parquet.Encoding encoding) {
    return Encoding.valueOf(encoding.name());
  }

  public static Type getType(Parquet.Type type) {
    return Type.valueOf(type.name());
  }

  public static CompressionCodec getType(Parquet.CompressionCodec codec) {
    return CompressionCodec.valueOf(codec.name());
  }

  public static void magicNumber(OutputStream o) throws IOException {
    o.write(MAGIC_NUMBER);
  }

  public static class DataPageInfo {
    public final PageHeader header;

    public DataPageInfo(Parquet.Encoding encoding,
                        int valueCount, int nullCount, int rowCount,
                        int uncompressedSize, int compressedSize,
                        int repLevelsSize, int defLevelsSize)
    {
      this.header
        = new PageHeader(PageType.DATA_PAGE_V2,
                         uncompressedSize, compressedSize);
      DataPageHeaderV2 dpHeader
        = new DataPageHeaderV2(valueCount, nullCount, rowCount,
                               getEncoding(encoding),
                               defLevelsSize, repLevelsSize);
      this.header.setData_page_header_v2(dpHeader);
    }

    public void writeTo(OutputStream out) throws IOException {
      Util.writePageHeader(header, out);
    }
  }

  public static class ColumnInfo {
    public final List<String> path;
    public final Type type;

    // TODO: Move this into ChunkBuffer and ChunkInfo
    public final List<Encoding> encodings;

    public ColumnInfo(String name,
                      Parquet.Type type, Parquet.Encoding encoding)
    {
      String[] path = { name };
      Encoding[] encodings = { getEncoding(encoding) };
      this.path = Arrays.asList(path);
      this.type = getType(type);
      this.encodings = Arrays.asList(encodings);
    }
  }

  public static class ChunkInfo {
    public final long chunkOffset;
    public final int valueCount;
    public final int uncompressedSize;
    public final int compressedSize;

    public ChunkInfo(long chunkOffset, int valueCount,
                     int uncompressedSize, int compressedSize)
    {
      this.chunkOffset = chunkOffset;
      this.valueCount = valueCount;
      this.uncompressedSize = uncompressedSize;
      this.compressedSize = compressedSize;
    }
  }

  public static class RowInfo {
    final int rowCount;
    final List<ChunkInfo> cols;
    public RowInfo(int rowCount, List<ChunkInfo> cols) {
      this.rowCount = rowCount;
      this.cols = cols;
    }
  }

  public static void writeFooter(OutputStream out,
                                 long offset,
                                 List<ColumnInfo> cols,
                                 List<RowInfo> rows)
    throws IOException
  {
    // Construct RowGroups
    List<RowGroup> groups = new ArrayList<RowGroup>(rows.size());
    long totalRowCount = 0;
    for (RowInfo ri: rows) {
      long byteCount = 0;
      List<ColumnChunk> chunks = new ArrayList<ColumnChunk>(ri.cols.size());
      for (int i = 0; i < cols.size(); i++) {
        ColumnInfo colI = cols.get(i);
        ChunkInfo chunkI = ri.cols.get(i);
        ColumnChunk c = new ColumnChunk(chunkI.chunkOffset);
        // c.file_path = unsupported: Everything must be in same file
        c.meta_data
          = new ColumnMetaData(colI.type, colI.encodings, colI.path,
                               CompressionCodec.UNCOMPRESSED,
                               chunkI.valueCount,
                               chunkI.uncompressedSize,
                               chunkI.compressedSize,
                               chunkI.chunkOffset);

        // Looking at ColumnChunkPageWriteStore.writeToFileWriter and
        // the calls it makes to ParquetFileWriter, it seems like
        // dictionary and firstData page offsets get set to the same.
        // Looking at ParquetMetadataConverter.addRowGroup, the
        // dictionary_page_offset of column metadata is always set,
        // even if there isn't a dictionary.  So this mimics the
        // observed logic:
        c.meta_data.dictionary_page_offset = chunkI.chunkOffset;

        chunks.add(c);
        byteCount += chunkI.uncompressedSize; // TODO: Right thing to add?
      }
      RowGroup rg = new RowGroup(chunks, byteCount, ri.rowCount);
      groups.add(rg);
      totalRowCount += ri.rowCount;
    }

    // Now make FileMetaData
    FileMetaData fmd = new FileMetaData(VERSION,
                                        toParquetSchema(cols),
                                        totalRowCount,
                                        groups);
    fmd.setCreated_by(CREATED_BY);

    // Now write the file metadata, capturing its size
    CountingOutputStream o = new CountingOutputStream(out);
    Util.writeFileMetaData(fmd, o);

    // Write out the size of the metadata (assume it's an int)
    int fmdSize = (int)o.getCount();
    out.write(fmdSize);
    out.write(fmdSize >> 8);
    out.write(fmdSize >> 16);
    out.write(fmdSize >> 24);

    magicNumber(out);
  }

  public static List<SchemaElement> toParquetSchema(List<ColumnInfo> cols) {
    List<SchemaElement> result = new ArrayList<SchemaElement>(cols.size());
    for (ColumnInfo ci: cols) {
        SchemaElement e = new SchemaElement(ci.path.get(0));
        e.setRepetition_type(FieldRepetitionType.REQUIRED);
        e.setType(ci.type);
        result.add(e);
    }
    return result;
  }
}
