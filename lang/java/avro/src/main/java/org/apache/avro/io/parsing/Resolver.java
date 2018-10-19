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
package org.apache.avro.io.parsing;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import static org.apache.avro.io.parsing.Resolver.ErrorAction.Type.*;

public class Resolver {
  /**
   * Returns a {link Resolver.Action} tree for resoling the writer
   * schema <tt>writer</tt> and the reader schema <tt>reader</tt>.
   *
   * @param writer    The schema used by the writer
   * @param reader    The schema used by the reader
   * @return          Nested actions for resolving the two
   */
  public static Action resolve(Schema writer, Schema reader) {
    return resolve(writer, reader, new HashMap<>());
  }

  private static Action resolve(Schema w, Schema r, Map<Pair, Action> seen) {
    final Schema.Type wType = w.getType();
    final Schema.Type rType = r.getType();

    if (wType == Schema.Type.UNION) {
      List<Schema> branches = w.getTypes();
      int sz = branches.size();
      Action[] actions = new Action[sz];
      for (int i = 0; i < sz; i++) actions[i] = resolve(branches.get(i), r, seen);
      return new WriterUnion(w, r, actions);
    }

    if (wType == rType) {
      switch (wType) {
      case NULL: case BOOLEAN:
      case INT: case LONG: case FLOAT: case DOUBLE:
      case STRING: case BYTES:
        return new DoNothing(w, r);

      case FIXED:
        if (w.getFullName() != null && ! w.getFullName().equals(r.getFullName()))
          return new ErrorAction(w, r, NAMES_DONT_MATCH);
        else if (w.getFixedSize() != r.getFixedSize())
          return new ErrorAction(w, r, SIZES_DONT_MATCH);
        else return new DoNothing(w, r);

      case ARRAY:
        Action et = resolve(w.getElementType(), r.getElementType(), seen);
        return new ContainerAction(w, r, et);

      case MAP:
        Action vt = resolve(w.getValueType(), r.getValueType(), seen);
        return new ContainerAction(w, r, vt);

      case ENUM:
        return EnumAdjust.resolve(w, r);

      case RECORD:
        return RecordAdjust.resolve(w, r, seen);

      default:
        throw new IllegalArgumentException("Unknown type for schema: " + wType);
      }
    } else if (rType == Schema.Type.UNION) return ReaderUnion.resolve(w, r, seen);
    else return Promote.resolve(w, r);
  }

  /**
   * An abstract class for an action to be taken to resolve a writer's
   * schema (found in public instance variable <tt>writer</tt>)
   * against a reader's schema (in <tt>reader</tt>).  Ordinarily,
   * neither field can be <tt>null</tt>, except that the
   * <tt>reader</tt> field can be <tt>null</tt> in a {@link
   * SkipAction}, which is used to skip a field in a writer's record
   * that doesn't exist in the reader's (and thus there is no reader
   * schema to resolve to).
   */
  public static abstract class Action {
    public final Schema writer, reader;
    protected Action(Schema w, Schema r) { writer = w; reader = r; }
  }

  /**
   * In this case, there's nothing to be done for resolution: the two
   * schemas are effectively the same.
   */
  public static class DoNothing extends Action {
    public DoNothing(Schema w, Schema r) { super(w, r); }
  }

  /**
   * In this case there is an error.  We put error Actions into trees
   * because Avro reports these errors in a lazy fashion: if a
   * particular input doesn't "tickle" the error (typically because
   * it's in a branch of a union that isn't found in the data being
   * read), then it's safe to ignore it.
   */
  public static class ErrorAction extends Action {
    public static enum Type {
        /** Use when Schema types don't match and can't be converted.  For
         * example, resolving "int" and "enum". */
        INCOMPATIBLE_SCHEMA_TYPES,

        /** Use when Schema types match but, in the case of record, enum,
         * or fixed, the names don't match. */
        NAMES_DONT_MATCH,

        /** Use when two fixed types match and their names match by their
         * sizes don't. */
        SIZES_DONT_MATCH,

        /** Use when matching two records and the reader has a field
         * with no default value and that field is missing in the
         * writer.. */
        MISSING_REQUIRED_FIELD,

        /** Use when matching a reader's union against a non-union and
         * can't find a branch that matches. */
        NO_MATCHING_BRANCH
    }

    public final Type error;

    public ErrorAction(Schema w, Schema r, Type e) {
      super(w,r);
      this.error = e;
    }
  }

  /**
   * In this case, the writer's type needs to be promoted to the
   * reader's.  These are constructed by {@link Promote.resolve},
   * which will only construct one when the writer's and reader's
   * schemas are different (ie, no "self promotion"), and whent the
   * promotion is one allowed by the Avro spec.
   */
  public static class Promote extends Action {
    private Promote(Schema w, Schema r) { super(w, r); }

    /**
     * Return a promotion.
     * @param w Writer's schema
     * @param r Rearder's schema
     * @result a {@link Promote} schema if the two schemas are compatible,
     * or {@link ErrorAction.Type.INCOMPATIBLE_SCHEMA_TYPE} if they are not.
     * @throws IllegalArgumentException if <em>getType()</em> of the two schemas
     * are not different.
     */
    public static Action resolve(Schema w, Schema r) {
      if (w.getType()== r.getType())
        throw new IllegalArgumentException("Only use when reader and writer are different.");
      if (isValid(w, r)) return new Promote(w,r);
      else return new ErrorAction(w, r, INCOMPATIBLE_SCHEMA_TYPES);
    }

    /** Returns true iff <tt>w</tt> and <tt>r</tt> are both
     * primitive types and either they are the same type or
     * <tt>w</tt> is promotable to <tt>r</tt>.
     */
    public static boolean isValid(Schema w, Schema r) {
      final Schema.Type wt = w.getType();
      switch (r.getType()) {
      case NULL: return true;
      case LONG:
        switch (wt) {
        case INT: case LONG: return true;
        default: break;
        }
      case FLOAT:
        switch (wt) {
        case INT: case LONG: case FLOAT: return true;
        default: break;
        }
      case DOUBLE:
        switch (wt) {
        case INT: case LONG: case FLOAT: case DOUBLE: return true;
        default: break;
        }
      case BYTES:
        switch (wt) {
        case STRING: case BYTES: return true;
        default: break;
        }
      case STRING:
        switch (wt) {
        case BYTES: case STRING: return true;
        default: break;
        }
      }
      return false;
    }
  }

  /** 
   * Used for array and map schemas: the public instance variable
   * <tt>elementAction</tt> contains the resolving action needed for
   * the element type of an array or value top of a map.
   */
  public static class ContainerAction extends Action {
    public final Action elementAction;
    public ContainerAction(Schema w, Schema r, Action e) {
      super(w, r);
      this.elementAction = e;
    }
  }

  /**
   * Contains information needed to resolve enumerations.  When
   * resolving enums, adjustments need to be made in two scenarios:
   * the index for an enum symbol might be different in the reader or
   * writer, or the reader might not have a symbol that was written
   * out for the writer (which is an error, but one we can only detect
   * when decoding data).
   *
   * These adjustments are reflected in the instance variable
   * <tt>adjustments</tt>.  For the symbol with index <tt>i</tt> in
   * the writer's enum definition, <tt>adjustments[i]</tt> -- and
   * integer -- contains the adjustment for that symbol.  If the
   * integer is positive, then reader also has the symbol and the
   * integer is its index in the reader's schema.  If
   * <tt>adjustment[i]</tt> is negative, then the reader does
   * <em>not</em> have the corresponding symbol (which is the error case).
   *
   * Sometimes there's no adjustments needed: all symbols in the
   * reader have the same index in the reader's and writer's schema.
   * This is a common case, and it allows for some optimization.  To
   * signal that this is the case, <tt>adjustments</tt> is set to
   * null.
   */
  public static class EnumAdjust extends Action {
    public final int[] adjustments;

    private EnumAdjust(Schema w, Schema r, int[] adj) {
      super(w, r);
      this.adjustments = adj;
    }

    /** If writer and reader don't have same name, a {@link
     * ErrorAction.Type.NAMES_DONT_MATCH} is returned, otherwise an
     * appropriate {@link EnumAdjust} is.
     */
    public static Action resolve(Schema w, Schema r) {
      if (w.getFullName() != null && ! w.getFullName().equals(r.getFullName()))
        return new ErrorAction(w, r, NAMES_DONT_MATCH);

      final List<String> wsymbols = w.getEnumSymbols();
      final List<String> rsymbols = r.getEnumSymbols();
      final int defaultIndex
        = (r.getEnumDefault() == null ? -1 : rsymbols.indexOf(r.getEnumDefault()));
      int[] adjustments = new int[wsymbols.size()];
      boolean noAdjustmentsNeeded = true;
      for (int i = 0; i < adjustments.length; i++) {
        int j = rsymbols.indexOf(wsymbols.get(i));
        adjustments[i] = (0 <= j ? j : defaultIndex);
        noAdjustmentsNeeded &= (i == j);
      }
      return new EnumAdjust(w, r, (noAdjustmentsNeeded ? null : adjustments));
    }
  }

  /**
   * This only appears inside {@link RecordAdjust.fieldActions}, i.e.,
   * the actions for adjusting the fields of a record.  This action
   * indicates that the writer's schema has a field that the reader's
   * does <em>not</em> have, and thus the field should be skipped.
   * Since there is no corresponding reader's schema for the writer's
   * in this case, the {@link Action.reader} field is <tt>null</tt>
   * for this subclass.
   */
  public static class SkipAction extends Action {
    public SkipAction(Schema w) { super(w, null); }
  }

  /**
   *  Instructions for resolving two record schemas.  Includes
   *  instructions on how to recursively resolve each field, an
   *  indication of when to skip (writer fields), plus information
   *  about which reader fields should be populated by defaults
   *  (because the writer doesn't have corresponding fields).
   */
  public static class RecordAdjust extends Action {
    /** An action for each field of the writer.  If the corresponding
     *  field is to be skipped during reading, then this will contain
     *  a {@link SkipAction}.  For fields to be read into the reading
     *  datum, will contain a regular action for resolving the
     *  writer/reader schemas of the matching fields. */
    public final Action[] fieldActions;

    /** Contains (all of) the reader's fields.  The first <i>n</i> of
       * these are the fields that will be read from the writer: these
       * <i>n</i> are in the order dictated by writer's schema.  The
       * remaining <i>m</i> fields will be read from default values
       * (actions for these default values are found in {@link
       * defaults}. */
    public final Field[] readerOrder;

    /** Pointer into {@link readerOrder} of the first reader field
     *  whose value comes from a default value. */
    public final int firstDefault;

    private RecordAdjust(Schema w, Schema r, Action[] fa, Field[] ro, int firstD) {
      super(w, r);
      this.fieldActions = fa;
      this.readerOrder = ro;
      this.firstDefault = firstD;
    }

    /**
     * Returns a {@link RecordAdjust} for the two schemas, or an
     * {@link ErrorAction} if there was a problem resolving.  An
     * {@link ErrorAction} is returned when either the two
     * record-schemas don't have the same name, or if the writer is
     * missing a field for which the reader does not have a default
     * value.
     * @throws RuntimeException if writer and reader schemas are not both records
     */
    static Action resolve(Schema w, Schema r, Map<Pair, Action> seen) {
      Pair wr = new Pair(w, r);
      Action result = seen.get(wr);
      if (result != null) return result;

      if (w.getFullName() != null && ! w.getFullName().equals(r.getFullName())) {
        result = new ErrorAction(w, r, NAMES_DONT_MATCH);
        seen.put(wr, result);
        return result;
      }

      List<Field> wfields = w.getFields();
      List<Field> rfields = r.getFields();

      Action[] actions = new Action[wfields.size()];
      Field[] reordered = new Field[rfields.size()];
      int firstDefault = 0;
      for (Schema.Field wf : wfields)
        if (r.getField(wf.name()) != null) firstDefault++;
      result = new RecordAdjust(w, r, actions, reordered, firstDefault);
      seen.put(wr, result); // Insert early to handle recursion

      int i = 0; int ridx = 0; for (Field wField : wfields) {
        Field rField = r.getField(wField.name());
        if (rField != null) {
          reordered[ridx++] = rField;
          actions[i++] = resolve(wField.schema(), rField.schema(), seen);
        } else actions[i++] = new SkipAction(wField.schema());
      }
      for (Field rf : rfields)
        if (w.getField(rf.name()) == null)
          if (rf.defaultValue() == null) {
            result = new ErrorAction(w, r, MISSING_REQUIRED_FIELD);
            seen.put(wr, result);
            return result;
          } else reordered[ridx++] = rf;
      return result;
    }
  }

  /** In this case, the writer was a union.  In this case, we resolve
   * the entire reader schema with _each_ branch of the writer schema
   * (stored in <tt>actions</tt>)..  Based on the tag we see in the
   * data stream, we pick the resolution branch selected by that tag.
   */
  public static class WriterUnion extends Action {
    public final Action[] actions;
    public WriterUnion(Schema w, Schema r, Action[] a) { super(w,r); actions = a; }
  }

  /**
   * In this case, the reader is a union and the writer is not.  For
   * this case, we need to pick the first branch of the reader that
   * matches the writer and pretend to the reader that the index of
   * this branch was found in the writer's data stream.
   *
   * To support this case, the {@link ReaderUnion} object has two
   * (public) fields: <tt>firstMatch</tt> gives the index of the first
   * matching branch in the reader's schema, and
   * <tt>actualResolution</tt> is the {@link Action} that resolves the
   * writer's schema with the schema found in the <tt>firstMatch</tt>
   * branch of the reader's schema.
   */
  public static class ReaderUnion extends Action {
    public final int firstMatch;
    public final Action actualResolution;

    public ReaderUnion(Schema w, Schema r, int firstMatch, Action actual) {
      super(w, r);
      this.firstMatch = firstMatch;
      this.actualResolution = actual;
    }

    /**
     * Returns a {@link ReaderUnion} action for resolving <tt>w</tt>
     * and <tt>r</tt>, or an {@link ErrorAction} if there is no branch
     * in the reader that matches the writer.
     * @throws RuntimeException if <tt>r</tt> is not a union schema or
     * <tt>w</tt> <em>is</em> a union schema
     */
    public static Action resolve(Schema w, Schema r, Map<Pair,Action> seen) {
      Schema.Type wt = w.getType();
      if (wt == Schema.Type.UNION)
        throw new IllegalArgumentException("Writer schema is union.");
      int i = 0; for (Schema b : r.getTypes()) {
        if (Promote.isValid(w, b))
          return new ReaderUnion(w, r, i, Promote.resolve(w, b));
        if (wt == b.getType())
          if (wt == Schema.Type.RECORD || wt == Schema.Type.ENUM || wt == Schema.Type.FIXED) {
            String wname = w.getFullName();
            if (wname != null && wname.equals(b.getFullName()))
              return new ReaderUnion(w, r, i, Resolver.resolve(w,b,seen));
          }
        i++;
      }
      return new ErrorAction(w, r, NO_MATCHING_BRANCH);
    }
  }

  private static class Pair {
    public Schema writer;
    public Schema reader;
    Pair(Schema w, Schema r) { writer = w; reader = r; }
  }
}
