package org.apache.avro.tool;

import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.fs.Path;

import org.apache.avro.io.parquet.ParquetEncoder;

// import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import org.apache.parquet.column.ParquetProperties;

// import org.apache.parquet.example.Paper;

public class ParquetTestTool implements Tool {
  public static void test() throws IOException {
    PrintWriter o = new PrintWriter(System.out,true);

    ParquetProperties props = ParquetProperties.builder().build();

    test1(props, t1);
    test1(props, t2);
    test1(props, t3);
    test1(props, t4);
  }

  public static void test1(ParquetProperties p, MessageType t)
    throws IOException
  {
    Path f = new Path(t.getName());
    ParquetEncoder e = new ParquetEncoder(f, t, p);
    e.close();
  }

  public static MessageType t1 =
    new MessageType("t1",
      new PrimitiveType(REQUIRED, INT64, "DocId")
    );

  public static MessageType t2 =
    new MessageType("t2",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "DocName")
    );

  public static MessageType t3 =
    new MessageType("t3",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(OPTIONAL, BINARY, "DocName")
    );

  public static MessageType t4 =
    new MessageType("t4",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "DocName"),
      new PrimitiveType(REPEATED, BINARY, "DocAlias")
    );

  @Override
  public int run(InputStream in,
                 PrintStream out,
                 PrintStream err,
                 List<String> args)
    throws Exception
  {
    test();
    return 0;
  }

  @Override
  public String getName() {
    return "parquet-test";
  }

  @Override
  public String getShortDescription() {
    return "Smoke tests for Parquet file output.";
  }
}
