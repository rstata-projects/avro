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
import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Resolver;
import org.apache.avro.Resolver.Action;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;
import org.apache.avro.specific.SpecificData;

/**
 * An "Advancer" is a tree of objects that apply resolution logic while reading
 * values out of a {@link Decoder}.
 *
 * An Advancer tree is created by calling {@link Advancer#from(Action)}. The
 * resulting tree mimics the reader schema of that Action object.
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

  public final Schema writer, reader;
  public final LogicalType logicalType;
  public final Conversion conversion;

  protected Advancer(Action a, LogicalType lt, Conversion conv) {
    this.writer = a.writer;
    this.reader = a.reader;
    this.logicalType = lt;
    this.conversion = conv;
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

  /**
   * Get index for a union. Note that this is to be used by
   * {@link getBranchAdvancer) only -- it does not neccessarily reflect the index
   * of the original schemas.
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
   * Build a {@link Advancer} tree that for a given {@link Action} tree. If input
   * argument (<code>a</code>) is a {@link Resolver.RecordAdjust}, the result is
   * guaranteed to be a {@link Advancer.Record}.
   */
  public static Advancer from(Action a) {
    return from(SpecificData.getForSchema(a.reader), a);
  }

  private static Advancer from(SpecificData d, Action a) {
    LogicalType lt = a.reader.getLogicalType();
    Conversion<?> conv = (lt == null ? null : d.getConversionFor(lt));
    switch (a.type) {
    case DO_NOTHING:
      switch (a.reader.getType()) {
      case NULL:
        return new NullFast(a, lt, conv);
      case BOOLEAN:
        return new BooleanFast(a, lt, conv);
      case INT:
        return new IntFast(a, lt, conv);
      case LONG:
        return new LongFast(a, lt, conv);
      case FLOAT:
        return new FloatFast(a, lt, conv);
      case DOUBLE:
        return new DoubleFast(a, lt, conv);
      case STRING:
        return new StringFast(a, lt, conv);
      case BYTES:
        return new BytesFast(a, lt, conv);
      case FIXED:
        return new FixedFast(a, lt, conv);
      default:
        throw new IllegalArgumentException("Unexpected schema for DoNothing:" + a.reader);
      }
    case PROMOTE:
      switch (((Resolver.Promote) a).promotion) {
      case INT2LONG:
        return new LongFromInt(a, lt, conv);
      case INT2FLOAT:
        return new FloatFromInt(a, lt, conv);
      case INT2DOUBLE:
        return new DoubleFromInt(a, lt, conv);
      case LONG2FLOAT:
        return new FloatFromLong(a, lt, conv);
      case LONG2DOUBLE:
        return new DoubleFromLong(a, lt, conv);
      case FLOAT2DOUBLE:
        return new DoubleFromFloat(a, lt, conv);
      case STRING2BYTES:
        return new BytesFromString(a, lt, conv);
      case BYTES2STRING:
        return new StringFromBytes(a, lt, conv);
      default:
        throw new IllegalArgumentException("Unexpected promotion:" + a);
      }
    case ENUM:
      Resolver.EnumAdjust e = (Resolver.EnumAdjust) a;
      if (e.noAdjustmentsNeeded)
        return new EnumFast(a, lt, conv);
      else
        return new EnumWithAdjustments(a, lt, conv, e.adjustments);

    case CONTAINER:
      Advancer ea = Advancer.from(d, ((Resolver.Container) a).elementAction);
      if (a.writer.getType() == Schema.Type.ARRAY)
        return new Array(a, lt, conv, ea);
      else
        return new Map(a, lt, conv, ea);

    case RECORD:
      return Advancer.Record.from(d, (Resolver.RecordAdjust) a, lt, conv);

    case WRITER_UNION:
      Resolver.WriterUnion wu = (Resolver.WriterUnion) a;
      Advancer[] branches = new Advancer[wu.actions.length];
      for (int i = 0; i < branches.length; i++) {
        Action ba = wu.actions[i];
        if (ba instanceof Resolver.ReaderUnion)
          branches[i] = Advancer.from(d, ((Resolver.ReaderUnion) ba).actualAction);
        else
          branches[i] = Advancer.from(d, ba);
      }
      if (a.reader.getType() == Schema.Type.UNION) {
        if (wu.unionEquiv)
          return new EquivUnion(a, lt, conv, branches);
        else
          return new UnionUnion(a, lt, conv, branches);
      }
      return new WriterUnion(a, lt, conv, branches);

    case READER_UNION:
      Resolver.ReaderUnion ru = (Resolver.ReaderUnion) a;
      return new ReaderUnion(a, lt, conv, ru.firstMatch, Advancer.from(d, ru.actualAction));

    case ERROR:
      return new Error(a, a.toString());
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

    public Error(Action a, String msg) {
      super(a, null, null);
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

    public Array(Action a, LogicalType lt, Conversion conv, Advancer ea) {
      super(a, lt, conv);
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
    private static Advancer KA;
    static {
      Schema s = Schema.create(Schema.Type.STRING);
      Action a = Resolver.resolve(s, s);
      KA = new StringFast(a, null, null);
    }

    public final Advancer keyAdvancer = KA;
    public final Advancer valAdvancer;

    public Map(Action a, LogicalType lt, Conversion conv, Advancer va) {
      super(a, lt, conv);
      this.valAdvancer = va;
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

  private static class NullFast extends Advancer {
    public NullFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public Object nextNull(Decoder in) throws IOException {
      in.readNull();
      return null;
    }
  }

  private static class BooleanFast extends Advancer {
    public BooleanFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public boolean nextBoolean(Decoder in) throws IOException {
      return in.readBoolean();
    }
  }

  private static class IntFast extends Advancer {
    public IntFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public int nextInt(Decoder in) throws IOException {
      return in.readInt();
    }
  }

  private static class LongFast extends Advancer {
    public LongFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public long nextLong(Decoder in) throws IOException {
      return in.readLong();
    }
  }

  private static class FloatFast extends Advancer {
    public FloatFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public float nextFloat(Decoder in) throws IOException {
      return in.readFloat();
    }
  }

  private static class DoubleFast extends Advancer {
    public DoubleFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public double nextDouble(Decoder in) throws IOException {
      return in.readDouble();
    }
  }

  private static class StringFast extends Advancer {
    public StringFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public String nextString(Decoder in) throws IOException {
      return in.readString();
    }

    public Utf8 nextString(Decoder in, Utf8 old) throws IOException {
      return in.readString(old);
    }
  }

  private static class BytesFast extends Advancer {
    public BytesFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      return in.readBytes(old);
    }
  }

  private static class FixedFast extends Advancer {
    private final int len;

    public FixedFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
      this.len = a.writer.getFixedSize();
    }

    public byte[] nextFixed(Decoder in, byte[] bytes, int start, int len) throws IOException {
      in.readFixed(bytes, start, len);
      return bytes;
    }
  }

  private static class EnumFast extends Advancer {
    public EnumFast(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public int nextEnum(Decoder in) throws IOException {
      return in.readEnum();
    }
  }

  //// The following set of subclasses apply promotion logic
  //// to the underlying value read.

  private static class LongFromInt extends Advancer {
    public LongFromInt(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public long nextLong(Decoder in) throws IOException {
      return (long) in.readInt();
    }
  }

  private static class FloatFromInt extends Advancer {
    public FloatFromInt(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public float nextFloat(Decoder in) throws IOException {
      return (float) in.readInt();
    }
  }

  private static class FloatFromLong extends Advancer {
    public FloatFromLong(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public float nextFloat(Decoder in) throws IOException {
      return (long) in.readLong();
    }
  }

  private static class DoubleFromInt extends Advancer {
    public DoubleFromInt(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readInt();
    }
  }

  private static class DoubleFromLong extends Advancer {
    public DoubleFromLong(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readLong();
    }
  }

  private static class DoubleFromFloat extends Advancer {
    public DoubleFromFloat(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public double nextDouble(Decoder in) throws IOException {
      return (double) in.readFloat();
    }
  }

  private static class BytesFromString extends Advancer {
    public BytesFromString(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
    }

    public ByteBuffer nextBytes(Decoder in, ByteBuffer old) throws IOException {
      Utf8 s = in.readString(null);
      return ByteBuffer.wrap(s.getBytes(), 0, s.getByteLength());
    }
  }

  private static class StringFromBytes extends Advancer {
    public StringFromBytes(Action a, LogicalType lt, Conversion conv) {
      super(a, lt, conv);
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

    public EnumWithAdjustments(Action a, LogicalType lt, Conversion conv, int[] adjustments) {
      super(a, lt, conv);
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

    public WriterUnion(Action a, LogicalType lt, Conversion conv, Advancer[] b) {
      super(a, lt, conv);
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

    public UnionUnion(Action a, LogicalType lt, Conversion conv, Advancer[] b) {
      super(a, lt, conv);
      branches = b;
    }

    public int nextIndex(Decoder in) throws IOException {
      int i = in.readIndex();
      if (branches[i] instanceof Error)
        branches[i].nextIndex(in); // Force error
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
    public EquivUnion(Action a, LogicalType lt, Conversion conv, Advancer[] b) {
      super(a, lt, conv, b);
    }

    public int nextIndex(Decoder in) throws IOException {
      return in.readIndex();
    }
  }

  private static class ReaderUnion extends Advancer {
    private int branch;
    private Advancer advancer;

    public ReaderUnion(Action a, LogicalType lt, Conversion conv, int b, Advancer adv) {
      super(a, lt, conv);
      branch = b;
      advancer = adv;
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

    private Record(Action a, LogicalType lt, Conversion conv, Advancer[] adv, Schema[] s, Schema.Field[] o, boolean f) {
      super(a, lt, conv);
      this.advancers = adv;
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

    protected static Advancer from(SpecificData d, Resolver.RecordAdjust ra, LogicalType lt, Conversion conv) {
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
        Action fa = ra.fieldActions[i++];
        Advancer fieldAdv = Advancer.from(d, fa);
        if (toSkip.length != 0)
          fieldAdv = new RecordField(fa, fieldAdv.logicalType, fieldAdv.conversion, toSkip, fieldAdv);
        fieldAdvs[nrf] = fieldAdv;
      }

      // Deal with any trailing fields to be skipped:
      Schema[] finalSkips = collectSkips(ra.fieldActions, i);
      // Assert i == ra.fieldActions.length

      // Deal with defaults
      for (int df = 0; rf < readOrder.length; rf++, df++, nrf++) {
        LogicalType flt = ra.readerOrder[rf].schema().getLogicalType();
        Conversion fconv = (lt == null ? null : d.getConversionFor(lt));
        fieldAdvs[nrf] = new Default(ra, flt, fconv, ra.defaults[df]);
      }

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

      return new Record(ra, lt, conv, fieldAdvs, finalSkips, readOrder, inOrder);
    }
  }

  private static class RecordField extends Advancer {
    private final Schema[] toSkip;
    private final Advancer field;

    public RecordField(Action a, LogicalType lt, Conversion conv, Schema[] toSkip, Advancer field) {
      super(a, lt, conv);
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

    private Default(Action a, LogicalType lt, Conversion conv, Object v) {
      super(a, lt, conv);
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
