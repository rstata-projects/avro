/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.avro.benchmark;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.benchmark.stages.BenchmarkStage;
import org.apache.avro.benchmark.stages.GenericNested;
import org.apache.avro.benchmark.stages.GenericStrings;
import org.apache.avro.benchmark.stages.GenericTest;
import org.apache.avro.benchmark.stages.GenericWithDefault;
import org.apache.avro.benchmark.stages.GenericWithOutOfOrder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

@State(Scope.Benchmark)
public class ReaderBenchmark {

    enum ReaderType {
      GENERIC_STANDARD,
      GENERIC_WITH_FASTREAD
    }


    @Param({ "GenericTest", "GenericStrings", "GenericNested", "GenericWithDefault", "GenericWithOutOfOrder" } )
    private String stageName;

    @Param({ "Generic", "GenericFastRead" })
    private String readerImplementation;

    private int count = 1000;

    private BenchmarkStage currentStage;
    private byte[] data;
    private Schema readerSchema;
    private Schema writerSchema;
    private DatumReader<Object> reader;

    @Setup
    public void setup() throws IOException {
      this.currentStage = getBenchmarkStage( stageName );
      this.data = currentStage.getSerializedTestData( count );
      this.readerSchema= currentStage.getReaderSchema();
      this.writerSchema = currentStage.getWriterSchema();
      this.reader = getDatumReader( readerImplementation, readerSchema, writerSchema );
    }

    private BenchmarkStage<? extends Object> getBenchmarkStage( String name ) {
      switch ( name ) {
        case "GenericTest" : return new GenericTest();
        case "GenericStrings" : return new GenericStrings();
        case "GenericNested" : return new GenericNested();
        case "GenericWithDefault" : return new GenericWithDefault();
        case "GenericWithOutOfOrder" : return new GenericWithOutOfOrder();
        default: return null;
      }
    }

    private DatumReader<Object> getDatumReader( String implementation, Schema readerSchema, Schema writerSchema ) {
      switch ( implementation ) {
        case "Generic" : {
          return new GenericData().createDatumReader( writerSchema, readerSchema );
        }
        case "GenericFastRead" : {
          GenericData data = new GenericData();
          data.setFastReaderEnabled(true);
          return data.createDatumReader( writerSchema, readerSchema );
        }
        default:
          return null;
      }
    }

    @Benchmark
    public DatumReader<Object> testReaderBuilding() {
      return getDatumReader( readerImplementation, readerSchema, writerSchema );
    }

    // @Benchmark
    public void testRead( Blackhole blackhole ) throws IOException {
      Decoder decoder = DecoderFactory.get().binaryDecoder( this.data, null );
      Object record = null;
      for ( int i = 0; i < 1000; i++ ) {
        record = reader.read( record, decoder );
        blackhole.consume( record );
      }
    }

}
