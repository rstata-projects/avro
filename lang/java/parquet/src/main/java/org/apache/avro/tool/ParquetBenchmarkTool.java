/*
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
package org.apache.avro.tool;

import org.apache.avro.tool.parquet.benchmark.Bench;

import java.io.InputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;

public class ParquetBenchmarkTool implements Tool {
  @Override
  public int run(InputStream in, PrintStream out, PrintStream err, List<String> args) throws Exception {
    if (args.size() < 2) {
      return usage();
    }
    Bench.Type type = null;
    try {
      type = Bench.Type.valueOf(args.get(0).toUpperCase());
    } catch (IllegalArgumentException e) {
      System.err.println("Invalid type: " + args.get(0) + ". Valid values: " + Arrays.toString(Bench.Type.values()));
      return 1;
    }
    if (type != Bench.Type.READ && args.size() < 3) {
      return usage();
    }
    String src = args.get(1);
    String dst = args.size() > 2 ? args.get(2) : null;
    int repeat = args.size() > 3 ? Integer.parseInt(args.get(3)) : 1;
    Bench.benchmark(type, src, dst, repeat);
    return 0;
  }

  private int usage() {
    System.err.println("Usage: parquet-benchmark type inputfile [outputfile] [repeat-count]");
    return 1;
  }

  @Override
  public String getName() {
    return "parquet-benchmark";
  }

  @Override
  public String getShortDescription() {
    return "Benchmarks Avro to Parquet conversion perfromance.";
  }
}
