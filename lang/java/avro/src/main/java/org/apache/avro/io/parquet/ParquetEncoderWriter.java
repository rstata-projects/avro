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
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.PublicColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class ParquetEncoderWriter {
  private final MessageType type;
  private final ParquetProperties props;
  private final ParquetFileWriter parquetFileWriter;
  private final CodecFactory cfact;
  private final CodecFactory.BytesCompressor compressor;
  private final ResettingColumnWriteStore columnStore;

  private PublicColumnChunkPageWriteStore pageStore;

  private long nextRowGroupSize;
  private long recordCountForNextMemCheck;
  private int recsThisGroup;


  public ParquetEncoderWriter(Path f, MessageType t, ParquetProperties p)
    throws IOException
  {
    Configuration hconf = new Configuration();
    this.type = t;
    this.props = p;
    this.parquetFileWriter = new ParquetFileWriter(hconf, t, f);
    this.parquetFileWriter.start();
    this.cfact = new CodecFactory(hconf, p.getPageSizeThreshold());
    this.compressor = cfact.getCompressor(CompressionCodecName.UNCOMPRESSED);
    this.columnStore = new ResettingColumnWriteStore(t);
  }

  public void start() throws IOException { newRowGroup(); }
  public ColumnWriteStore getColumnStore() { return columnStore; }

  public void endRecord() {
    columnStore.endRecord();
    recsThisGroup++;
  }

  public void close() throws IOException {
    flush();
    parquetFileWriter.end(new HashMap<String,String>());
    cfact.release();
  }

  public void flush() throws IOException {
    // Copied from InternalParquetRecordWriter.flushRowGroupToStore
    if (recsThisGroup > 0) {
      parquetFileWriter.startBlock(recsThisGroup);
      columnStore.flush();
      pageStore.flushToFileWriter(parquetFileWriter);
      parquetFileWriter.endBlock();
      newRowGroup();
    }
  }

  private void newRowGroup() throws IOException {
    this.recsThisGroup = 0;
    this.nextRowGroupSize = parquetFileWriter.getNextRowGroupSize();
    this.recordCountForNextMemCheck = props.getMinRowCountForPageSizeCheck();
    pageStore = new PublicColumnChunkPageWriteStore(compressor, type,
                                                    props.getAllocator());
    columnStore.reset(props.newColumnWriteStore(type, pageStore));
  }

  private void checkBlockSizeReached() throws IOException {
    // Copied from InternalParquetRecordWriter.checkBlockSizeReached
    if (recsThisGroup >= recordCountForNextMemCheck) {
      // checking the memory size is relatively expensive, so let's not
      // do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recsThisGroup;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        long tmp = recsThisGroup / 2; // Try to get half-way this time
        flush();
        recordCountForNextMemCheck = recsToNextCheck(tmp);
      } else {
        long tmp = (long)(nextRowGroupSize / ((float)recordSize));
        recordCountForNextMemCheck = recsThisGroup + recsToNextCheck(tmp);
      }
    }
  }

  private long recsToNextCheck(long estimate) {
    estimate = Math.max(estimate, props.getMinRowCountForPageSizeCheck());
    estimate = Math.min(estimate, props.getMaxRowCountForPageSizeCheck());
    return estimate;
  }


  static class ResettingColumnWriteStore implements ColumnWriteStore {
    private final Map<ColumnDescriptor, ResettingColumnWriter> writers;
    private ColumnWriteStore underlying;

    public ResettingColumnWriteStore(MessageType t) {
      this.writers = new HashMap<ColumnDescriptor, ResettingColumnWriter>();
      for (ColumnDescriptor col: t.getColumns()) {
        writers.put(col, new ResettingColumnWriter(col));
      }
    }

    public void reset(ColumnWriteStore underlying) {
      this.underlying = underlying;
      for (ResettingColumnWriter r: writers.values())
        r.reset(underlying);
    }

    public void endRecord() {
      underlying.endRecord();
    }

    public long getAllocatedSize() { return underlying.getAllocatedSize(); }
    public long getBufferedSize() { return underlying.getBufferedSize(); }
    public String memUsageString() { return underlying.memUsageString(); }
    public void close() { underlying.close(); }
    public void flush() { underlying.flush(); }

    public ColumnWriter getColumnWriter(ColumnDescriptor column) {
      return writers.get(column);
    }

    protected class ResettingColumnWriter implements ColumnWriter {
      ColumnWriter del;
      final ColumnDescriptor col;

      public ResettingColumnWriter(ColumnDescriptor col) { this.col = col; }

      public void reset(ColumnWriteStore underlying) {
        this.del = underlying.getColumnWriter(col);
      }

      public void write(int value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void write(long value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void write(boolean value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void write(Binary value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void write(float value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void write(double value, int repetitionLevel, int definitionLevel)
        { del.write(value, repetitionLevel, definitionLevel); }

      public void writeNull(int repetitionLevel, int definitionLevel)
        { del.writeNull(repetitionLevel, definitionLevel); }

      public void close() { del.close(); }

      public long getBufferedSizeInMemory()
        { return del.getBufferedSizeInMemory(); }
    }
  }
}
