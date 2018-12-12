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

package org.apache.parquet.internal.column.columnindex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveComparator;
import org.apache.parquet.schema.PrimitiveType;

class ColumnIndexValidator {

  public enum Contract {
    MIN_SMALLER_THAN_ALL_VALUES,
    MAX_LARGER_THAN_ALL_VALUES,
    NULL_COUNT_CORRECT,
    // TODO: More
  }

  public static class ContractViolation {
    public ContractViolation(Contract violatedContract, String violatingValueAsString, String constraintValueAsString,
        int rowGroupNumber, int columnNumber, int pageNumber) {
      this.violatedContract = violatedContract;
      this.violatingValueAsString = violatingValueAsString;
      this.constraintValueAsString = constraintValueAsString;
      this.rowGroupNumber = rowGroupNumber;
      this.columnNumber = columnNumber;
      this.pageNumber = pageNumber;
    }
    public Contract violatedContract;
    public String violatingValueAsString;
    public String constraintValueAsString;
    public int rowGroupNumber;
    public int columnNumber;
    public int pageNumber;
  }
  
  private static abstract class PageValidator {
    static PageValidator createPageValidator(
        PrimitiveType type,
        int rowGroupNumber,
        int columnNumber,
        int pageNumber,
        List<ContractViolation> violations,
        ColumnReader columnReader,
        long nullCount,
        ByteBuffer minValue,
        ByteBuffer maxValue) {
      PageValidator pageValidator;
      switch (type.getPrimitiveTypeName()) {
        case FLOAT:
          pageValidator = new FloatPageValidator(minValue, maxValue);
          break;
        default:
          throw new UnsupportedOperationException(String.format("Validation of %s type is not implemented", type));
      }
      pageValidator.comparator = type.comparator();
      pageValidator.columnReader = columnReader;
      pageValidator.rowGroupNumber = rowGroupNumber;
      pageValidator.columnNumber = columnNumber;
      pageValidator.pageNumber = pageNumber;
      pageValidator.nullCountExpected = nullCount;
      pageValidator.nullCountActual = 0;
      pageValidator.maxDefinitionLevel = columnReader.getDescriptor().getMaxDefinitionLevel();
      pageValidator.violations = violations;
      return pageValidator;
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
      validateContract(nullCountExpected == nullCountActual,
          Contract.NULL_COUNT_CORRECT, 
          Long.toString(nullCountExpected),
          Long.toString(nullCountActual));
    }

    void validateContract(boolean contractCondition, 
        Contract type, 
        String violatingValueAsString, 
        String constraintValueAsString) {
      if (!contractCondition) {
        violations.add(new ContractViolation(type, violatingValueAsString, constraintValueAsString, rowGroupNumber, columnNumber, pageNumber));
      }
    }

    abstract void validateValue();
    
    protected PrimitiveComparator<?> comparator;
    protected int rowGroupNumber;
    protected int columnNumber;
    protected int pageNumber;
    protected int maxDefinitionLevel;
    protected long nullCountExpected;
    protected long nullCountActual;
    protected ColumnReader columnReader;
    protected List<ContractViolation> violations;
  }
  
  private static class FloatPageValidator extends PageValidator {
    public FloatPageValidator(ByteBuffer minValue, ByteBuffer maxValue) {
      this.minValue = minValue.getFloat();
      this.maxValue = maxValue.getFloat();
    }
    private float minValue;
    private float maxValue;

    void validateValue() {
      float value = columnReader.getFloat();
      validateContract(comparator.compare(value, minValue) <= 0,
          Contract.MIN_SMALLER_THAN_ALL_VALUES, 
          Float.toString(value),
          Float.toString(minValue));
      validateContract(comparator.compare(value, maxValue) >= 0,
          Contract.MAX_LARGER_THAN_ALL_VALUES, 
          Float.toString(value),
          Float.toString(minValue));
    }
  }

  public List<ContractViolation> checkContractViolations(InputFile file) throws IOException {
    List<ContractViolation> violations = new ArrayList<>();
    ParquetFileReader reader = ParquetFileReader.open(file); 
    FileMetaData meta = reader.getFooter().getFileMetaData();
    MessageType schema = meta.getSchema();
    List<ColumnDescriptor> columns = schema.getColumns();

    List<BlockMetaData> blocks = reader.getFooter().getBlocks();
    int rowGroupNumber = 0;
    PageReadStore rowGroup = reader.readNextRowGroup();
    ColumnReadStore columnReadStore = 
        new ColumnReadStoreImpl(rowGroup, new DummyRecordConverter(schema).getRootConverter(), schema, null);
    while (rowGroup != null) {
      List<ColumnChunkMetaData> columnChunks = blocks.get(rowGroupNumber).getColumns();
      assert(columnChunks.size() == columns.size());
      int columnNumber = 0;
      while (columnNumber < columns.size()) {
        ColumnDescriptor column = columns.get(columnNumber);
        ColumnChunkMetaData columnChunk = columnChunks.get(columnNumber);
        ColumnIndex columnIndex = reader.readColumnIndex(columnChunk);
        OffsetIndex offsetIndex = reader.readOffsetIndex(columnChunk);
        List<ByteBuffer> minValues = columnIndex.getMinValues();
        List<ByteBuffer> maxValues = columnIndex.getMaxValues();
        // BoundaryOrder boundaryOrder = columnIndex.getBoundaryOrder();
        List<Long> nullCounts = columnIndex.getNullCounts();
        // List<Boolean> nullPages = columnIndex.getNullPages();
        long rowNumber = 0;
        for (int pageNumber = 0; pageNumber < offsetIndex.getPageCount(); ++pageNumber) {
          PageValidator pageValidator = PageValidator.createPageValidator(
              column.getPrimitiveType(), 
              rowGroupNumber, columnNumber, pageNumber,
              violations, columnReadStore.getColumnReader(column), 
              nullCounts.get(pageNumber),
              minValues.get(pageNumber),
              maxValues.get(pageNumber)); 
          long lastRowNumberInPage = offsetIndex.getLastRowIndex(pageNumber, rowGroup.getRowCount());
          while (rowNumber < lastRowNumberInPage) {
            pageValidator.validateValuesBelongingToRow();
            ++rowNumber;
          }
          pageValidator.finishPage();
        }
        columnNumber++;
      }
      rowGroup = reader.readNextRowGroup();
      rowGroupNumber++;
    }
    return violations;
  }
}
