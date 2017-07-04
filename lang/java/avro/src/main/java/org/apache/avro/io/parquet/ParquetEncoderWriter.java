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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.PublicColumnChunkPageWriteStore;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;


public class ParquetEncoderWriter {
  private static final Logger LOG
    = LoggerFactory.getLogger(ParquetEncoderWriter.class);

  private final MessageType type;
  private final ParquetProperties props;
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
    this.parquetFileWriter = new ParquetFileWriter(hconf, t, f);
    this.parquetFileWriter.start();
    this.cfact = new CodecFactory(hconf, p.getPageSizeThreshold());
    this.compressor = cfact.getCompressor(CompressionCodecName.GZIP);
    newRowGroup();
  }

  public ColumnWriteStore getColumnWriteStore() { return columnStore; }

  public boolean endRecord() throws IOException {
    columnStore.endRecord();
    recsThisGroup++;
    return checkBlockSizeReached();
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
  }

  private boolean checkBlockSizeReached() throws IOException {
    // Copied from InternalParquetRecordWriter.checkBlockSizeReached
    if (recsThisGroup >= recordCountForNextMemCheck) {
      // checking the memory size is relatively expensive, so let's not
      // do it for every record.
      long memSize = columnStore.getBufferedSize();
      long recordSize = memSize / recsThisGroup;
      // flush the row group if it is within ~2 records of the limit
      // it is much better to be slightly under size than to be over at all
      if (memSize > (nextRowGroupSize - 2 * recordSize)) {
        LOG.info("mem size {} > {}: flushing {} records to disk.",
                 memSize, nextRowGroupSize, recsThisGroup);
        long tmp = recsThisGroup / 2; // Try to get half-way this time
        flush();
        recordCountForNextMemCheck = recsToNextCheck(tmp);
        return true;
      } else {
        long tmp = (long)(nextRowGroupSize / ((float)recordSize));
        recordCountForNextMemCheck = recsThisGroup + recsToNextCheck(tmp);
        LOG.debug("Checked mem at {} will check again at: {}",
                  recsThisGroup, recordCountForNextMemCheck);
      }
    }
    return false;
  }

  private long recsToNextCheck(long estimate) {
    estimate = Math.max(estimate, props.getMinRowCountForPageSizeCheck());
    estimate = Math.min(estimate, props.getMaxRowCountForPageSizeCheck());
    return estimate;
  }
}
