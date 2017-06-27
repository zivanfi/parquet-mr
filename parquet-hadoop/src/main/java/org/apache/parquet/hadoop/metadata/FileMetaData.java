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
package org.apache.parquet.hadoop.metadata;

import static java.util.Collections.unmodifiableMap;
import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.parquet.format.ColumnOrder;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * File level meta data (Schema, codec, ...)
 *
 * @author Julien Le Dem
 *
 */
public final class FileMetaData implements Serializable {
  private static final long serialVersionUID = 1L;

  private final MessageType schema;

  private final Map<String, String> keyValueMetaData;

  private final String createdBy;

  private final List<ColumnOrder> columnOrders;

  private static final Logger LOG = LoggerFactory.getLogger(FileMetaData.class);

  /**
   * @deprecated Use FileMetaData(MessageType, Map<String, String>,
   *   String, List<ColumnOrder>) instead.
   *
   * @param schema the schema for the file
   * @param keyValueMetaData the app specific metadata
   * @param createdBy the description of the library that created the file
   */
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy) {
    this(schema, keyValueMetaData, createdBy, null);
  }

  /**
   * @param schema the schema for the file
   * @param keyValueMetaData the app specific metadata
   * @param createdBy the description of the library that created the file
   * @param columnOrders sort order used for each column in this file
   */
  public FileMetaData(MessageType schema, Map<String, String> keyValueMetaData, String createdBy,
      List<ColumnOrder> columnOrders) {
    super();
    this.schema = checkNotNull(schema, "schema");
    this.keyValueMetaData = unmodifiableMap(checkNotNull(keyValueMetaData, "keyValueMetaData"));
    this.createdBy = createdBy;
    int columnCount = schema.getFieldCount();
    if (columnOrders != null && columnOrders.size() != columnCount) {
      LOG.warn("Ignoring invalid column_orders (size does not match column count).");
      columnOrders = null;
    }
    if (columnOrders == null) {
      columnOrders = new ArrayList<>(Collections.nCopies(columnCount, (ColumnOrder) null));
    }
    this.columnOrders = columnOrders;
  }

  /**
   * @return the schema for the file
   */
  public MessageType getSchema() {
    return schema;
  }

  /**
   * @return the column orders for the file
   */
  public List<ColumnOrder> getColumnOrders() {
    return columnOrders;
  }

  @Override
  public String toString() {
    return String.format("FileMetaData{schema: %s, metadata: %s, columnOrders: %s}",
        schema, keyValueMetaData, columnOrders);
  }

  /**
   * @return meta data for extensions
   */
  public Map<String, String> getKeyValueMetaData() {
    return keyValueMetaData;
  }

  /**
   * @return the description of the library that created the file
   */
  public String getCreatedBy() {
    return createdBy;
  }

}
