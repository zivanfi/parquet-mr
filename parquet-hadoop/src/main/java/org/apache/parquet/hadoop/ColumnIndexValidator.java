/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.parquet.hadoop;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveStringifier;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ColumnIndexValidator {

  public enum Contract {
    MIN_LTEQ_VALUE("The min value stored in the index must be less than or equal to all values."),
    MAX_GTEQ_VALUE("The max value stored in the index must be greater than or equal to all values."),
    NULL_COUNT_CORRECT("The null count stored in the index must be equal to the number of nulls."),
    NULL_PAGE_HAS_NO_VALUES("Only pages consisting entirely of NULL-s can be marked as a null page in the index.");
    // TODO: More
    public final String description;

    Contract(String description) {
      this.description = description;
    }
  }

  public static class ContractViolation {
    public ContractViolation(Contract violatedContract, String valueFromIndex, String valueFromPage,
        int rowGroupNumber, int columnNumber, int pageNumber) {
      this.violatedContract = violatedContract;
      this.valueFromIndex = valueFromIndex;
      this.valueFromPage = valueFromPage;
      this.rowGroupNumber = rowGroupNumber;
      this.columnNumber = columnNumber;
      this.pageNumber = pageNumber;
    }

    public Contract violatedContract;
    public String valueFromIndex;
    public String valueFromPage;
    public int rowGroupNumber;
    public int columnNumber;
    public int pageNumber;

    @Override
    public String toString() {
      return String.format("Contract violation\nLocation: row group %d, column %d, page %d\nViolated contract: %s\nCorresponding value from the index: %s\nOffending value from the page: %s\n",
          rowGroupNumber, columnNumber, pageNumber,
          violatedContract.description,
          valueFromIndex,
          valueFromPage);
    }
  }

  private static abstract class PageValidator {
    static PageValidator createPageValidator(
        PrimitiveType type,
        int rowGroupNumber,
        int columnNumber,
        int pageNumber,
        List<ContractViolation> violations,
        ColumnReader columnReader,
        ByteBuffer minValue,
        ByteBuffer maxValue,
        long nullCount,
        boolean isNullPage) {
      PageValidator pageValidator = createTypeSpecificValidator(type.getPrimitiveTypeName(), minValue, maxValue);
      pageValidator.comparator = type.comparator();
      pageValidator.stringifier = type.stringifier();
      pageValidator.columnReader = columnReader;
      pageValidator.rowGroupNumber = rowGroupNumber;
      pageValidator.columnNumber = columnNumber;
      pageValidator.pageNumber = pageNumber;
      pageValidator.nullCountInIndex = nullCount;
      pageValidator.nullCountActual = 0;
      pageValidator.isNullPage = isNullPage;
      pageValidator.maxDefinitionLevel = columnReader.getDescriptor().getMaxDefinitionLevel();
      pageValidator.violations = violations;
      return pageValidator;
    }

    private static PageValidator createTypeSpecificValidator(PrimitiveTypeName type, ByteBuffer minValue,
        ByteBuffer maxValue) {
      switch (type) {
      case BINARY:
      case FIXED_LEN_BYTE_ARRAY:
        return new BinaryPageValidator(minValue, maxValue);
      case BOOLEAN:
        return new BooleanPageValidator(minValue, maxValue);
      case DOUBLE:
        return new DoublePageValidator(minValue, maxValue);
      case FLOAT:
        return new FloatPageValidator(minValue, maxValue);
      case INT32:
        return new IntPageValidator(minValue, maxValue);
      case INT64:
        return new LongPageValidator(minValue, maxValue);
      default:
        throw new UnsupportedOperationException(String.format("Validation of %s type is not implemented", type));
      }
    }

    void validateValuesBelongingToRow() {
      do {
        if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
          validateValue();
        } else {
          ++nullCountActual;
        }
        columnReader.consume();
      } while (columnReader.getCurrentRepetitionLevel() != 0);
    }

    public void finishPage() {
      validateContract(nullCountInIndex == nullCountActual,
          Contract.NULL_COUNT_CORRECT,
          () -> Long.toString(nullCountActual),
          () -> Long.toString(nullCountInIndex));
    }

    void validateContract(boolean contractCondition,
        Contract type,
        Supplier<String> valueFromIndex,
        Supplier<String> valueFromPage) {
      if (!contractCondition) {
        violations.add(
            new ContractViolation(type, valueFromIndex.get(), valueFromPage.get(), rowGroupNumber,
                columnNumber, pageNumber));
      }
    }

    abstract void validateValue();

    protected PrimitiveComparator<Binary> comparator;
    protected PrimitiveStringifier stringifier;
    protected int rowGroupNumber;
    protected int columnNumber;
    protected int pageNumber;
    protected int maxDefinitionLevel;
    protected long nullCountInIndex;
    protected long nullCountActual;
    protected boolean isNullPage;
    protected ColumnReader columnReader;
    protected List<ContractViolation> violations;
  }

  private static class BinaryPageValidator extends PageValidator {
    private Binary minValue;
    private Binary maxValue;

    public BinaryPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = Binary.fromConstantByteBuffer(minValue);
      this.maxValue = Binary.fromConstantByteBuffer(maxValue);
    }

    void validateValue() {
      Binary value = columnReader.getBinary();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  private static class BooleanPageValidator extends PageValidator {
    private boolean minValue;
    private boolean maxValue;

    public BooleanPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.get(0) != 0;
      this.maxValue = maxValue.get(0) != 0;
    }

    void validateValue() {
      boolean value = columnReader.getBoolean();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  private static class DoublePageValidator extends PageValidator {
    private double minValue;
    private double maxValue;

    public DoublePageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.getDouble();
      this.maxValue = maxValue.getDouble();
    }

    void validateValue() {
      double value = columnReader.getDouble();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  private static class FloatPageValidator extends PageValidator {
    private float minValue;
    private float maxValue;

    public FloatPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.getFloat();
      this.maxValue = maxValue.getFloat();
    }

    void validateValue() {
      float value = columnReader.getFloat();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  private static class IntPageValidator extends PageValidator {
    private int minValue;
    private int maxValue;

    public IntPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.getInt();
      this.maxValue = maxValue.getInt();
    }

    void validateValue() {
      int value = columnReader.getInteger();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  private static class LongPageValidator extends PageValidator {
    private long minValue;
    private long maxValue;

    public LongPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.getLong();
      this.maxValue = maxValue.getLong();
    }

    void validateValue() {
      long value = columnReader.getLong();
      validateContract(!isNullPage,
          Contract.NULL_PAGE_HAS_NO_VALUES,
          () -> stringifier.stringify(value),
          () -> "N/A");
      validateContract(comparator.compare(value, minValue) >= 0,
          Contract.MIN_LTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
      validateContract(comparator.compare(value, maxValue) <= 0,
          Contract.MAX_GTEQ_VALUE,
          () -> stringifier.stringify(value),
          () -> stringifier.stringify(minValue));
    }
  }

  public static List<ContractViolation> checkContractViolations(InputFile file) throws IOException {
    List<ContractViolation> violations = new ArrayList<>();
    ParquetFileReader reader = ParquetFileReader.open(file);
    FileMetaData meta = reader.getFooter().getFileMetaData();
    MessageType schema = meta.getSchema();
    List<ColumnDescriptor> columns = schema.getColumns();

    List<BlockMetaData> blocks = reader.getFooter().getBlocks();
    int rowGroupNumber = 0;
    PageReadStore rowGroup = reader.readNextRowGroup();
    while (rowGroup != null) {
      ColumnReadStore columnReadStore = new ColumnReadStoreImpl(rowGroup,
          new DummyRecordConverter(schema).getRootConverter(), schema, null);
      List<ColumnChunkMetaData> columnChunks = blocks.get(rowGroupNumber).getColumns();
      assert (columnChunks.size() == columns.size());
      for (int columnNumber = 0; columnNumber < columns.size(); ++columnNumber) {
        ColumnDescriptor column = columns.get(columnNumber);
        ColumnChunkMetaData columnChunk = columnChunks.get(columnNumber);
        ColumnIndex columnIndex = reader.readColumnIndex(columnChunk);
        if (columnIndex == null) {
          // TODO: we shall still check the offset index
          continue;
        }
        OffsetIndex offsetIndex = reader.readOffsetIndex(columnChunk);
        List<ByteBuffer> minValues = columnIndex.getMinValues();
        List<ByteBuffer> maxValues = columnIndex.getMaxValues();
        // BoundaryOrder boundaryOrder = columnIndex.getBoundaryOrder();
        List<Long> nullCounts = columnIndex.getNullCounts();
        List<Boolean> nullPages = columnIndex.getNullPages();
        long rowNumber = 0;
        ColumnReader columnReader = columnReadStore.getColumnReader(column);
        for (int pageNumber = 0; pageNumber < offsetIndex.getPageCount(); ++pageNumber) {
          PageValidator pageValidator = PageValidator.createPageValidator(
              column.getPrimitiveType(),
              rowGroupNumber, columnNumber, pageNumber,
              violations, columnReader,
              minValues.get(pageNumber),
              maxValues.get(pageNumber),
              nullCounts.get(pageNumber),
              nullPages.get(pageNumber));
          long lastRowNumberInPage = offsetIndex.getLastRowIndex(pageNumber, rowGroup.getRowCount());
          while (rowNumber <= lastRowNumberInPage) {
            pageValidator.validateValuesBelongingToRow();
            ++rowNumber;
          }
          pageValidator.finishPage();
        }
      }
      rowGroup = reader.readNextRowGroup();
      rowGroupNumber++;
    }
    return violations;
  }
}
