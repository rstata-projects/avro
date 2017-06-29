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

import org.apache.avro.io.parsing.ParquetGrammar;
import org.apache.avro.io.parsing.Symbol;

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
  private final ParquetGrammar grammar;
  private final ParquetFileWriter parquetFileWriter;
  private final CodecFactory cfact;
  private final CodecFactory.BytesCompressor compressor;

  private PublicColumnChunkPageWriteStore pageStore;
  private ColumnWriteStore columnStore;

  private long nextRowGroupSize;
  private long recordCountForNextMemCheck;
  private int recsThisGroup;


  public ParquetEncoderWriter(Path f, MessageType t, ParquetProperties p)
    throws IOException
  {
    Configuration hconf = new Configuration();
    this.type = t;
    this.props = p;
    this.grammar = new ParquetGrammar(t);
    this.parquetFileWriter = new ParquetFileWriter(hconf, t, f);
    this.parquetFileWriter.start();
    this.cfact = new CodecFactory(hconf, p.getPageSizeThreshold());
    this.compressor = cfact.getCompressor(CompressionCodecName.UNCOMPRESSED);
    newRowGroup();
  }

  public Symbol getRoot() { return grammar.root; }

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
    recsThisGroup = 0;
    nextRowGroupSize = parquetFileWriter.getNextRowGroupSize();
    recordCountForNextMemCheck = props.getMinRowCountForPageSizeCheck();
    pageStore = new PublicColumnChunkPageWriteStore(compressor, type,
                                                    props.getAllocator());
    columnStore = props.newColumnWriteStore(type, pageStore);
    grammar.resetWriters(columnStore);
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
}
