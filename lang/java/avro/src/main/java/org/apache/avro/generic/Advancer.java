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
package org.apache.avro.generic;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.avro.AvroTypeException;
import org.apache.avro.Resolver;
import org.apache.avro.Resolver.Action;
import org.apache.avro.Schema;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

/**
 * An "Advancer" is a tree of objects that apply resolution logic while reading
 * values out of a {@link Decoder}.
 *
 * An Advancer tree is created by calling
 * {@link Advancer#from(Action)}. The resulting tree mimics the reader
 * schema of that Action object.
 *
 * A decoder for the reader schema is meant to traverse that schema in a
 * depth-first fashion. When it hits a leaf of type <code>Xyz</code>, it should
 * call corresponding <code>nextXyx</code> on the Advancer. For example, if the
 * reader hits a leaf indicating that an integer should be read, it should call
 * {@link Advancer#nextInt}, as in <code>a.nextInt(in)</code>, where
 * <code>a</code> is the advancer being traversed, and <code>in</code> is the
 * Decoder being read from.
 *
 * When traversing an record, array, or map, the decoder should case the
 * corresponding Advancer to {@link Advancer.Record}, {@link Advancer.Array}, or
 * {@link Advancer.Map} respectively. See the Javadoc for those classes for more
 * details.
 *
 * For unions, the decoder should call {@link Advancer#nextIndex} to fetch the
 * branch and then {@link Advancer#getBranchAdvancer} to get the advancer of
 * that branch.
 *
 * For an illustration of how to use this class, please refer to the
 * implementation of {@link GenericDatumReader2}.
 **/
public abstract class Advancer {

  //// API methods of Advancer. Used by decoding methods to
  //// read values out of Decoder, applying resolution logic
  //// in the process. In the base class, these do throw
  //// a not-supported exception. Specific subclasses implement
  //// certain ones, e.g., IntFast (the Advancer used when
  //// an integer is read with no promotion) overrides just
  //// readInt.

  public final DataFactory dataFactory;
  public final Schema writer, reader;
  public final LogicalType logicalType
  public final Conversion<?> conversion;

  protected Advancer(DataFactory df, Schema w, Schema r, LogicalType lt, Conversion<?> c) {
    dataFactory = df;
    writer = w;
    reader = r;
    logicalType = lt;
    conversion = c;
  }

  /**
   * The {@link Error} subclass overrides this method and throws an
   * AvroTypeException instead.
   */
  protected Exception exception() {
    throw new UnsupportedOperationException();
  }

  public Object nextNull(Decoder in) throws IOException {
    exception();
    return null;
  }

  public boolean nextBoolean(Decoder in) throws IOException {
    exception();
    return false;
  }

  public int nextInt(Decoder in) throws IOException {
    exception();
    return 0;
  }

  public long nextLong(Decoder in) throws IOException {
    exception();
    return 0;
  }

  public float nextFloat(Decoder in) throws IOException {
    exception();
    return 0;
  }

  public double nextDouble(Decoder in) throws IOException {
    exception();
    return 0;
  }

  public int nextEnum(Decoder in) throws IOException {
    exception();
    return 0;
  }

  public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
    exception();
    return null;
  }

  public String nextString(Decoder in) throws IOException {
    exception();
    return null;
  }

  public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
    exception();
    return null;
  }

  public byte[] nextFixed(Decoder in, byte[] bytes, int start, int length) throws IOException {
    exception();
    return null;
  }

  public byte[] nextFixed(Decoder in, byte[] bytes) throws IOException {
    return nextFixed(in, bytes, 0, bytes.length);
  }

  /** Get index for a union.  Note that this is to be used by
   * {@link getBranchAdvancer) only -- it does not neccessarily
   * reflect the index of the original schemas.
   */
  public int nextIndex(Decoder in) throws IOException {
    exception();
    return 0;
  }

  /**
   * Access to contained advancer for unions. You must call
   * {@link Advancer#nextIndex} before calling this method.
   */
  public Advancer getBranchAdvancer(Decoder in, int index) throws IOException {
    exception();
    return null;
  }

  ////// Here's the builder for Advancer trees. The subclasses used by
  ////// this implementation are found below.

  /**
   * Build a {@link Advancer} tree that for a given {@link Action} tree.
   * If input argument (<code>a</code>) is a {@link Resolver.RecordAdjust}, the
   * result is guaranteed to be a {@link Advancer.Record}.
   */
  public static Advancer from(Action a, DataFactory df) {
    LogicalType lt = a.reader.getLogicalType();
    Conversion<?> conv = (lt == null ? null : df.getConversionFor(lt);
    switch (a.type) {
    case DO_NOTHING:
      switch (a.reader.getType()) {
      case NULL:
        return Null.from(a, df, lt, conv);
      case BOOLEAN:
        return Boolean.from(a, df, lt, conv);
      case INT:
        return Int.from(a, df, lt, conv);
      case LONG:
        return Long.from(a, df, lt, conv);
      case FLOAT:
        return Float.from(a, df, lt, conv);
      case DOUBLE:
        return Double.from(a, df, lt, conv);
      case STRING:
        return String.from(a, df, lt, conv);
      case BYTES:
        return Bytes.from(a, df, lt, conv);
      case FIXED:
        return new Fixed.from(a, df, lt, conv);
      default:
        throw new IllegalArgumentException("Unexpected schema for DoNothing:" + a.reader);
      }
    case PROMOTE:
      switch (((Resolver.Promote) a).promotion) {
      case INT2LONG:
        return LongFromInt.from(a, df, lt, conv);
      case INT2FLOAT:
        return FloatFromInt.from(a, df, lt, conv);
      case INT2DOUBLE:
        return DoubleFromInt.from(a, df, lt, conv);
      case LONG2FLOAT:
        return FloatFromLong.from(a, df, lt, conv);
      case LONG2DOUBLE:
        return DoubleFromLong.from(a, df, lt, conv);
      case FLOAT2DOUBLE:
        return DoubleFromFloat.from(a, df, lt, conv);
      case STRING2BYTES:
        return BytesFromString.from(a, df, lt, conv);
      case BYTES2STRING:
        return StringFromBytes.from(a, df, lt, conv);
      default:
        throw new IllegalArgumentException("Unexpected promotion:" + a);
      }

    case ENUM:
      return Enum.from((Resolver.EnumAdjust) a, df, lt, conv);

    case CONTAINER:
      if (a.writer.getType() == Schema.Type.ARRAY)
        return Array.from((Resolver.Container) a, df, lt, conv);
      else
        return Map.from((Resolver.Container) a, df, lt, conv);

    case RECORD:
      return Advancer.Record.from((Resolver.RecordAdjust) a, df, lt, conv);

    case WRITER_UNION:
      Resolver.WriterUnion wu = (Resolver.WriterUnion) a;
      Advancer[] branches = new Advancer[wu.actions.length];
      for (int i = 0; i < branches.length; i++) {
        Action ba = wu.actions[i];
        if (ba instanceof Resolver.ReaderUnion)
          branches[i] = Advancer.from(((Resolver.ReaderUnion) ba).actualAction);
        else
          branches[i] = Advancer.from(ba);
      }
      if (a.reader.getType() == Schema.Type.UNION) {
        if (wu.unionEquiv)
          return new EquivUnion(a.writer, a.reader, branches);
        else
          return new UnionUnion(a.writer, a.reader, branches);
      }
      return new WriterUnion(a.writer, a.reader, branches);

    case READER_UNION:
      Resolver.ReaderUnion ru = (Resolver.ReaderUnion) a;
      return new ReaderUnion(a.writer, a.reader, ru.firstMatch, Advancer.from(ru.actualAction));

    case ERROR:
      return new Error(a.writer, a.reader, a.toString());
    case SKIP:
      throw new RuntimeException("Internal error.  Skip should've been consumed.");
    default:
      throw new IllegalArgumentException("Unknown action:" + a);
    }
  }

  private static Schema[] collectSkips(Action[] actions, int start) {
    Schema[] result = EMPTY_SCHEMA_ARRAY;
    int j = start;
    while (j < actions.length && actions[j].type == Action.Type.SKIP)
      j++;
    if (start < j) {
      result = new Schema[j - start];
      for (int k = 0; k < (j - start); k++)
        result[k] = actions[start + k].writer;
    }
    return result;
  }

  private static final Schema[] EMPTY_SCHEMA_ARRAY = new Schema[0];

  ////// Subclasses of Advancer -- real work is done in these

  /**
   * All methods of <code>this</code> throw {@link AvroTypeException} with
   * appropriate message. Used for throwing resolution errors in a lazy fashion
   * (i.e., as actual data causes the error to manifest).
   */
  private static class Error extends Advancer {
    String msg;

    public Error(Schema w, Schema r, String msg) {
      super(w, r);
      this.msg = msg;
    }

    protected Exception exception() {
      throw new AvroTypeException(msg);
    }
  }

  /**
   * Used for Array. The following fragment illustrates how to use to read an
   * array of int:
   *
   * <pre>
   * Advancer.Array c = (Advancer.Array) advancer;
   * for (long i = c.firstChunk(in); i != 0; i = c.nextChunk(in)) {
   *   for (long j = 0; j < i; j++) {
   *     int element = c.elementAdvancer.readInt(in);
   *     // .. do something with this element
   *   }
   * }
   * </pre>
   * 
   * See the implementation of {@link GenericDatumReader2} for more illustrations.
   */
  public static class Array extends Advancer {
    public final Advancer elementAdvancer;

    public Array(Schema w, Schema r, Advancer ea) {
      super(w, r);
      elementAdvancer = ea;
    }

    public long firstChunk(Decoder in) throws IOException {
      return in.readArrayStart();
    }

    public long nextChunk(Decoder in) throws IOException {
      return in.arrayNext();
    }
  }

  /**
   * Used for Map. The following fragment illustrates how to use to read an array
   * of int:
   *
   * <pre>
   * Advancer.Map c = (Advancer.Map) advancer;
   * for (long i = c.firstChunk(in); i != 0; i = c.nextChunk(in)) {
   *   for (long j = 0; j < i; j++) {
   *     String key = c.keyAdvancer.readString(in);
   *     int val = c.valAdvancer.readInt(in);
   *     // .. do something with this element
   *   }
   * }
   * </pre>
   * 
   * See the implementation of {@link GenericDatumReader2} for more illustrations.
   */
  public static class Map extends Advancer {
    public final Advancer keyAdvancer = StringFast.INSTANCE;
    public final Advancer valAdvancer;

    protected Map(Schema w, Schema r, Advancer val) {
      super(w, r);
      this.valAdvancer = val;
    }

    public long firstChunk(Decoder in) throws IOException {
      return in.readMapStart();
    }

    public long nextChunk(Decoder in) throws IOException {
      return in.mapNext();
    }
  }

  //// The following set of subclasses are for when there is no
  //// resolution logic to be applied. All that needs to be done
  //// is call the corresponding method on the Decoder.

  private static class Null extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.NULL);
    private static final Null INSTANCE = new Null();

    private Null() {
        super(DataFactory.getDefault(), S, S, null, null);
    }

    public Object nextNull(Decoder in) throws IOException {
      in.readNull();
      return null;
    }

    public static Advancer from(Advancer a, DataFactory df, LogicalType lt, Conversion<?> c) {
      return Converter.from(INSTANCE, df, lt, c);
    }
  }

  private static class Boolean extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.BOOLEAN);
    public static final Boolean INSTANCE = new Boolean();

    private Boolean() {
      super(DataFactory.getDefault(), S, S, null, null);
    }

    public boolean nextBoolean(Decoder in) throws IOException {
      return in.readBoolean();
    }

    public static Advancer from(Advancer a, DataFactory df, LogicalType lt, Conversion<?> c) {
      return Converter.from(INSTANCE, df, lt, c);
    }
  }

  private static class Int extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.INT);
    public static final IntFast INSTANCE = new IntFast();

    private Int() {
      super(DataFactory.getDefault(), S, S, null, null);
    }

    public int nextInt(Decoder in) throws IOException {
      return in.readInt();
    }

    public static Advancer from(Advancer a, DataFactory df, LogicalType lt, Conversion<?> c) {
      return Converter.from(INSTANCE, df, lt, c);
    }
  }

  private static class LongFast extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.LONG);
    public static final LongFast INSTANCE = new LongFast();

    private LongFast() {
      super(S, S);
    }

    public long nextLong(Decoder in) throws IOException {
      return in.readLong();
    }
  }

  private static class FloatFast extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.FLOAT);
    public static final FloatFast INSTANCE = new FloatFast();

    private FloatFast() {
      super(S, S);
    }

    public float nextFloat(Decoder in) throws IOException {
      return in.readFloat();
    }
  }

  private static class DoubleFast extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.DOUBLE);
    public static final DoubleFast INSTANCE = new DoubleFast();

    private DoubleFast() {
      super(S, S);
    }

    public double nextDouble(Decoder in) throws IOException {
      return in.readDouble();
    }
  }

  private static class StringFast extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.STRING);
    public static final StringFast INSTANCE = new StringFast();

    private StringFast() {
      super(S, S);
    }

    public String nextString(Decoder in) throws IOException {
      return in.readString();
    }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return in.readString(old);
    }
  }

  private static class BytesFast extends Advancer {
    private static final Schema S = Schema.create(Schema.Type.BYTES);
    public static final BytesFast INSTANCE = new BytesFast();

    private BytesFast() {
      super(S, S);
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      return in.readBytes(old);
    }
  }

  private static class FixedFast extends Advancer {
    private final int len;

    private FixedFast(Schema w, Schema r) {
      super(w, r);
      this.len = w.getFixedSize();
    }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException {
      in.readFixed(bytes, start, len);
      return bytes;
    }
  }

  private static class EnumFast extends Advancer {
    public EnumFast(Schema w, Schema r) {
      super(w, r);
    }

    public int nextEnum(Decoder in) throws IOException {
      return in.readEnum();
    }
  }

  //// The following set of subclasses apply promotion logic
  //// to the underlying value read.

  private static class LongFromInt extends Advancer {
    public static final LongFromInt INSTANCE = new LongFromInt();

    private LongFromInt() {
      super(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.LONG));
    }

    public long nextLong(Decoder in) throws IOException {
      return (long) in.readInt();
    }
  }

  private static class FloatFromInt extends Advancer {
    public static final FloatFromInt INSTANCE = new FloatFromInt();

    private FloatFromInt() {
      super(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.FLOAT));
    }

    public float nextFloat(Decoder in) throws IOException {
      return (float) in.readInt();
    }
  }

  private static class FloatFromLong extends Advancer {
    public static final FloatFromLong INSTANCE = new FloatFromLong();

    private FloatFromLong() {
      super(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.FLOAT));
    }

    public float nextFloat(Decoder in) throws IOException {
      return (float) in.readLong();
    }
  }

  private static class DoubleFromInt extends Advancer {
    public static final DoubleFromInt INSTANCE = new DoubleFromInt();

    private DoubleFromInt() {
      super(Schema.create(Schema.Type.INT), Schema.create(Schema.Type.DOUBLE));
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readInt();
    }
  }

  private static class DoubleFromLong extends Advancer {
    public static final DoubleFromLong INSTANCE = new DoubleFromLong();

    private DoubleFromLong() {
      super(Schema.create(Schema.Type.LONG), Schema.create(Schema.Type.DOUBLE));
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readLong();
    }
  }

  private static class DoubleFromFloat extends Advancer {
    public static final DoubleFromFloat INSTANCE = new DoubleFromFloat();

    private DoubleFromFloat() {
      super(Schema.create(Schema.Type.FLOAT), Schema.create(Schema.Type.DOUBLE));
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readFloat();
    }
  }

  private static class BytesFromString extends Advancer {
    public static final BytesFromString INSTANCE = new BytesFromString();

    private BytesFromString() {
      super(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.BYTES));
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    }
  }

  private static class StringFromBytes extends Advancer {
    public static final StringFromBytes INSTANCE = new StringFromBytes();

    private StringFromBytes() {
      super(Schema.create(Schema.Type.BYTES), Schema.create(Schema.Type.STRING));
    }

    public String nextString(Decoder in) throws IOException {
      return new String(in.readBytes(null).array(), StandardCharsets.UTF_8);
    }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return new Utf8(in.readBytes(null).array());
    }
  }

  //// This last set of advancers are used when more sophisticated
  //// adjustmentds are needed

  private static class EnumWithAdjustments extends Advancer {
    private final int[] adjustments;

    public EnumWithAdjustments(Schema w, Schema r, int[] adjustments) {
      super(w, r);
      this.adjustments = adjustments;
    }

    public int nextEnum(Decoder in) throws IOException {
      return adjustments[in.readInt()];
    }
  }

  /**
   * In this case, the writer has a union by the reader doesn't, so we consume the
   * tag ourself and call the corresponding advancer.
   */
  private static class WriterUnion extends Advancer {
    private Advancer[] branches;

    public WriterUnion(Schema w, Schema r, Advancer[] b) {
      super(w, r);
      branches = b;
    }

    private final Advancer b(Decoder in) throws IOException {
      return branches[in.readIndex()];
    }

    public Object nextNull(Decoder in) throws IOException {
      return b(in).nextNull(in);
    }

    public boolean nextBoolean(Decoder in) throws IOException {
      return b(in).nextBoolean(in);
    }

    public int nextInt(Decoder in) throws IOException {
      return b(in).nextInt(in);
    }

    public long nextLong(Decoder in) throws IOException {
      return b(in).nextLong(in);
    }

    public float nextFloat(Decoder in) throws IOException {
      return b(in).nextFloat(in);
    }

    public double nextDouble(Decoder in) throws IOException {
      return b(in).nextDouble(in);
    }

    public int nextEnum(Decoder in) throws IOException {
      return b(in).nextEnum(in);
    }

    public String nextString(Decoder in) throws IOException {
      return b(in).nextString(in);
    }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return b(in).nextString(in, old);
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      return b(in).nextBytes(in, old);
    }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int length) throws IOException {
      return b(in).nextFixed(in, bytes, start, length);
    }

    public int nextIndex(Decoder in) throws IOException {
      return b(in).nextIndex(in);
    }

    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException {
      return b(in).getBranchAdvancer(in, branch);
    }
  }

  /**
   * In this case, reader and writer have the same union, so let the decoder
   * consume it as a regular union.
   */
  private static class UnionUnion extends Advancer {
    private final Advancer[] branches;

    public UnionUnion(Schema w, Schema r, Advancer[] b) {
      super(w, r);
      branches = b;
    }

    public int nextIndex(Decoder in) throws IOException {
      int i = in.readIndex();
      if (branches[i] instanceof Error) branches[i].nextIndex(in); // Force error
      return i;
    }

    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException {
      return branches[branch];
    }
  }

  /**
   * In this case, reader and writer have the same union, so let the decoder
   * consume it as a regular union.
   */
  private static class EquivUnion extends UnionUnion {
    public EquivUnion(Schema w, Schema r, Advancer[] b) {
      super(w, r, b);
    }

    public int nextIndex(Decoder in) throws IOException {
      return in.readIndex();
    }
  }

  private static class ReaderUnion extends Advancer {
    private int branch;
    private Advancer advancer;

    public ReaderUnion(Schema w, Schema r, int b, Advancer a) {
      super(w, r);
      branch = b;
      advancer = a;
    }

    public int nextIndex(Decoder in) {
      return branch;
    }

    public Advancer getBranchAdvancer(Decoder in, int b) {
      if (b != this.branch)
        throw new IllegalArgumentException("Branch much be " + branch + ", got " + b);
      return advancer;
    }
  }

  //// Records are particularly intricate because we may have to skip
  //// fields, read fields out of order, and use default values.

  /**
   * Advancer for records. The {@link Advancer.Record#advancers} array contains an
   * advancer for each field, ordered according writer (which determines the order
   * in which data must be read). The {@link Advancer.Record#readerOrder} array
   * tells you how those advancers line up with the reader's fields. See
   * {@link GenericDatumReader2} for guidance on how to use these fields.
   * 
   * Note that a decoder <em>must</em> call {@link Advancer.Record#done} after
   * interpreting all the elemnts in {@link Advancer.Record#advancers}.
   *
   * As a convenience, {@link Advancer.Record#inOrder} is set to true iff the
   * reader and writer order agrees (i.e., iff
   * <code>readerOrder[i].pos() == i</code> for all i). Generated code can use
   * this to optimize this common case.
   */
  public static class Record extends Advancer {
    public final Advancer[] advancers;
    private Schema[] finalSkips;
    public final Schema.Field[] readerOrder;
    public final boolean inOrder;

    private Record(Schema w, Schema r, Advancer[] a, Schema[] s, Schema.Field[] o, boolean f) {
      super(w, r);
      this.advancers = a;
      this.finalSkips = s;
      this.readerOrder = o;
      this.inOrder = f;
    }

    /**
     * Must be called after consuming all elements of
     * {@link Advancer.Record#advancers}.
     */
    public void done(Decoder in) throws IOException {
      ignore(finalSkips, in);
    }

    protected static Advancer from(Resolver.RecordAdjust ra) {
      /** Two cases: reader + writer agree on order, vs disagree. */
      /** This is the complicated case, since skipping is involved. */
      /** Special subclasses of Advance will encapsulate skipping. */

      // Compute the "readerOrder" argument to Advancer.Record constructor
      Schema.Field[] readOrder = ra.readerOrder;

      // Compute the "advancers" argument to Advancer.Record constructor
      Advancer[] fieldAdvs = new Advancer[readOrder.length];

      int i = 0; // Index into ra.fieldActions
      int rf = 0; // Index into ra.readerOrder
      int nrf = 0; // (Insertion) index into fieldAdvs

      // Deal with fields to be read
      for (; rf < ra.firstDefault; rf++, nrf++) {
        Schema[] toSkip = collectSkips(ra.fieldActions, i);
        i += toSkip.length;
        Advancer fieldAdv = Advancer.from(ra.fieldActions[i++]);
        if (toSkip.length != 0)
          fieldAdv = new RecordField(fieldAdv.writer, fieldAdv.reader, toSkip, fieldAdv);
        fieldAdvs[nrf] = fieldAdv;
      }

      // Deal with any trailing fields to be skipped:
      Schema[] finalSkips = collectSkips(ra.fieldActions, i);
      // Assert i == ra.fieldActions.length

      // Deal with defaults
      for (int df = 0; rf < readOrder.length; rf++, df++, nrf++)
        fieldAdvs[nrf] = new Default(ra.readerOrder[df].schema(), ra.defaults[df]);

      // If reader and writer orders agree, sort fieldAdvs by reader
      // order (i.e., move defaults into the correct place), to allow
      // decoders to have an optimized path for the common case of a
      // record's field order not changing.
      boolean inOrder = true;
      for (int k = 0; k < ra.firstDefault - 1; k++)
        inOrder &= (readOrder[k].pos() < readOrder[k + 1].pos());
      if (inOrder) {
        Advancer[] newAdvancers = new Advancer[fieldAdvs.length];
        Schema.Field[] newReadOrder = new Schema.Field[fieldAdvs.length];
        for (int k = 0, rf2 = 0, df = ra.firstDefault; k < readOrder.length; k++) {
          if (readOrder[rf2].pos() < readOrder[df].pos()) {
            newAdvancers[k] = fieldAdvs[rf2];
            newReadOrder[k] = readOrder[rf2++];
          } else {
            newAdvancers[k] = fieldAdvs[df];
            newReadOrder[k] = readOrder[df++];
          }
        }
        fieldAdvs = newAdvancers;
        readOrder = newReadOrder;
      }

      return new Record(ra.writer, ra.reader, fieldAdvs, finalSkips, readOrder, inOrder);
    }
  }

  private static class RecordField extends Advancer {
    private final Schema[] toSkip;
    private final Advancer field;

    public RecordField(Schema w, Schema r, Schema[] toSkip, Advancer field) {
      super(w, r);
      this.toSkip = toSkip;
      this.field = field;
    }

    public Object nextNull(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextNull(in);
    }

    public boolean nextBoolean(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextBoolean(in);
    }

    public int nextInt(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextInt(in);
    }

    public long nextLong(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextLong(in);
    }

    public float nextFloat(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextFloat(in);
    }

    public double nextDouble(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextDouble(in);
    }

    public int nextEnum(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextEnum(in);
    }

    public String nextString(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextString(in);
    }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      ignore(toSkip, in);
      return field.nextString(in, old);
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      ignore(toSkip, in);
      return field.nextBytes(in, old);
    }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException {
      ignore(toSkip, in);
      return field.nextFixed(in, bytes, start, len);
    }

    public int nextIndex(Decoder in) throws IOException {
      ignore(toSkip, in);
      return field.nextIndex(in);
    }

    public Advancer getBranchAdvancer(Decoder in, int branch) throws IOException {
      ignore(toSkip, in);
      return field.getBranchAdvancer(in, branch);
    }
  }

  private static class Default extends Advancer {
    protected final Object val;

    private Default(Schema s, Object v) {
      super(s, s);
      val = v;
    }

    public Object nextNull(Decoder in) {
      return val;
    }

    public boolean nextBoolean(Decoder in) {
      return (Boolean) val;
    }

    public int nextInt(Decoder in) {
      return (Integer) val;
    }

    public long nextLong(Decoder in) {
      return (Long) val;
    }

    public float nextFloat(Decoder in) {
      return (Float) val;
    }

    public double nextDouble(Decoder in) {
      return (Double) val;
    }

    public int nextEnum(Decoder in) {
      return (Integer) val;
    }

    // TODO -- finish for the rest of the types
  }


  private static void ignore(Schema[] toIgnore, Decoder in) throws IOException {
    for (Schema s : toIgnore)
      skip(s, in);
  }

  // Probably belongs someplace else, although Decoder doesn't reference
  // Schema, and Schema doesn't reference Decoder, and I'd hate to create
  // new dependencies...
  public static void skip(Schema s, Decoder in) throws IOException {
    switch (s.getType()) {
    case NULL:
      in.readNull();
      break;
    case BOOLEAN:
      in.readBoolean();
      break;
    case INT:
      in.readInt();
      break;
    case LONG:
      in.readLong();
      break;
    case FLOAT:
      in.readFloat();
      break;
    case DOUBLE:
      in.readDouble();
      break;
    case STRING:
      in.skipString();
      break;
    case BYTES:
      in.skipBytes();
      break;
    case FIXED:
      in.skipFixed(s.getFixedSize());
      break;
    case ENUM:
      in.readEnum();
      break;
    case UNION:
      skip(s.getTypes().get(in.readInt()), in);
      break;
    case RECORD:
      for (Schema.Field f : s.getFields())
        skip(f.schema(), in);
      break;
    case ARRAY:
      for (long i = in.skipArray(); i != 0; i = in.skipArray())
        for (long j = 0; j < i; j++)
          skip(s.getElementType(), in);
      break;
    case MAP:
      for (long k = in.skipArray(); k != 0; k = in.skipArray())
        for (long l = 0; l < k; l++) {
          in.skipString(); // Key
          skip(s.getValueType(), in);
        }
      break;
    default:
      throw new IllegalArgumentException("Unknown type for schema: " + s);
    }
  }
}
