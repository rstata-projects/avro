package org.apache.avro.tool;

import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.hadoop.fs.Path;

import org.apache.avro.io.parquet.ParquetEncoder;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.apache.parquet.schema.Type.Repetition.REPEATED;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

import org.apache.parquet.column.ParquetProperties;

// import org.apache.parquet.example.Paper;

public class ParquetTestTool implements Tool {
  public static PrintWriter o = new PrintWriter(System.out,true);

  public static void test() throws IOException {
    PrintWriter o = new PrintWriter(System.out,true);

    ParquetProperties props = ParquetProperties.builder().build();

    test0(props, t1);
    test0(props, t2);
    test0(props, t3);
    test0(props, t4);
    test0(props, t5);

    test1(props);
    test2(props);
    test3(props);
    test4(props);
    test5(props);
  }

  public static void test0(ParquetProperties p, MessageType t)
    throws IOException
  {
    Path f = new Path(t.getName() + ".0");
    ParquetEncoder e = new ParquetEncoder(f, t, p);
    e.close();
    o.println("done");
  }

  public static void test1(ParquetProperties p) throws IOException {
    Path f = new Path(t1.getName() + ".1");
    ParquetEncoder e = new ParquetEncoder(f, t1, p);
    e.writeLong(0L);
    e.close();
    o.println("done");

    f = new Path(t1.getName() + ".2");
    e = new ParquetEncoder(f, t1, p);
    e.writeLong(-1L);
    e.writeLong(1L);
    e.close();
    o.println("done");

    f = new Path(t1.getName() + ".3");
    e = new ParquetEncoder(f, t1, p);
    e.writeLong(Long.MIN_VALUE);
    e.writeLong(0L);
    e.writeLong(Long.MAX_VALUE);
    e.close();
    o.println("done");
  }

  public static void test2(ParquetProperties p) throws IOException {
    Path f = new Path(t2.getName() + ".1");
    ParquetEncoder e = new ParquetEncoder(f, t2, p);
    e.writeLong(0L);
    e.writeBytes("Doc-1321");
    e.close();
    o.println("done");

    f = new Path(t2.getName() + ".2");
    e = new ParquetEncoder(f, t2, p);
    e.writeLong(0L);
    e.writeBytes("Doc-0");
    e.writeLong(1L);
    e.writeBytes("Doc-1");
    e.close();
    o.println("done");

    f = new Path(t2.getName() + ".3");
    e = new ParquetEncoder(f, t2, p);
    e.writeLong(0L);
    e.writeBytes("");
    e.writeLong(1L);
    e.writeBytes("\0");
    e.writeLong(1L);
    e.writeBytes("\t");
    e.close();
    o.println("done");
  }

  public static void test3(ParquetProperties p) throws IOException {
    Path f = new Path(t3.getName() + ".1");
    ParquetEncoder e = new ParquetEncoder(f, t3, p);
    e.writeLong(0L);
    e.writeIndex(1);
    e.writeBytes("Doc-1321");
    e.close();
    o.println("done");

    f = new Path(t3.getName() + ".1n");
    e = new ParquetEncoder(f, t3, p);
    e.writeLong(0L);
    e.writeIndex(0);
    e.close();
    o.println("done");

    f = new Path(t3.getName() + ".2");
    e = new ParquetEncoder(f, t3, p);
    e.writeLong(0L);
    e.writeIndex(0);
    e.writeLong(1L);
    e.writeIndex(1);
    e.writeBytes("");
    e.close();
    o.println("done");

    f = new Path(t3.getName() + ".3");
    e = new ParquetEncoder(f, t3, p);
    e.writeLong(0L);
    e.writeIndex(0);
    e.writeLong(1L);
    e.writeIndex(0);
    e.writeLong(1L);
    e.writeIndex(1);
    e.writeBytes("\0");
    e.close();
    o.println("done");
  }

  public static void test4(ParquetProperties p) throws IOException {
    Path f = new Path(t4.getName() + ".10");
    ParquetEncoder e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".11");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".12");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeBytes("Alias-2");
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".13");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeBytes("Alias-2");
    e.writeBytes("Alias-3");
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".20");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.writeLong(1L);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".21");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeArrayEnd();
    e.writeLong(1L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t4.getName() + ".22");
    e = new ParquetEncoder(f, t4, p);
    e.writeLong(0L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeBytes("Alias-2");
    e.writeArrayEnd();
    e.writeLong(1L);
    e.writeArrayStart();
    e.writeBytes("Alias-1");
    e.writeBytes("Alias-2");
    e.writeBytes("Alias-3");
    e.writeArrayEnd();
    e.close();
    o.println("done");
  }

  public static void test5(ParquetProperties p) throws IOException {
    Path f = new Path(t5.getName() + ".10");
    ParquetEncoder e = new ParquetEncoder(f, t5, p);
    e.writeLong(0L);
    e.writeIndex(0);
    e.close();
    o.println("done");

    f = new Path(t5.getName() + ".11");
    e = new ParquetEncoder(f, t5, p);
    e.writeLong(Long.MAX_VALUE);
    e.writeIndex(1);
    e.writeIndex(0);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t5.getName() + ".12");
    e = new ParquetEncoder(f, t5, p);
    e.writeLong(Long.MIN_VALUE);
    e.writeIndex(1);
    e.writeIndex(1);
    e.writeBoolean(false);
    e.writeArrayStart();
    e.writeLong(1L);
    e.writeLong(2L);
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeArrayEnd();
    e.close();
    o.println("done");

    f = new Path(t5.getName() + ".30");
    e = new ParquetEncoder(f, t5, p);
    e.writeLong(0L);
    e.writeIndex(0);

    e.writeLong(1L);
    e.writeIndex(1);
    e.writeIndex(0);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeArrayEnd();

    e.writeLong(2L);
    e.writeIndex(1);
    e.writeIndex(1);
    e.writeBoolean(true);
    e.writeArrayStart();
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeArrayEnd();

    e.writeLong(3L);
    e.writeIndex(1);
    e.writeIndex(1);
    e.writeBoolean(false);
    e.writeArrayStart();
    e.writeLong(-1L);
    e.writeLong(-2L);
    e.writeLong(-3L);
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeArrayEnd();

    e.writeLong(4L);
    e.writeIndex(1);
    e.writeIndex(0);
    e.writeArrayStart();
    e.writeLong(1L);
    e.writeArrayEnd();
    e.writeArrayStart();
    e.writeLong(-2L);
    e.writeLong(-3L);
    e.writeLong(-4L);
    e.writeLong(-5L);
    e.writeLong(-6L);
    e.writeArrayEnd();
    e.close();
    o.println("done");
  }

  public static MessageType t1 =
    new MessageType("parquet-test1",
      new PrimitiveType(REQUIRED, INT64, "DocId")
    );

  public static MessageType t2 =
    new MessageType("parquet-test2",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REQUIRED, BINARY, "DocName")
    );

  public static MessageType t3 =
    new MessageType("parquet-test3",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(OPTIONAL, BINARY, "DocName")
    );

  public static MessageType t4 =
    new MessageType("parquet-test4",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new PrimitiveType(REPEATED, BINARY, "DocAlias")
    );

  public static MessageType t5 =
    new MessageType("parquet-test5",
      new PrimitiveType(REQUIRED, INT64, "DocId"),
      new GroupType(OPTIONAL, "Links",
        new PrimitiveType(OPTIONAL, BOOLEAN, "Fresh"),
        new PrimitiveType(REPEATED, INT64, "Backward"),
        new PrimitiveType(REPEATED, INT64, "Forward")
      )
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
