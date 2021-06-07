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

package com.flink.demo.cases.case02;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.java.io.jdbc.JDBCUtils.setRecordToStatement;

/**
 * OutputFormat to write Rows into a JDBC database.
 * The OutputFormat has to be configured using the supplied OutputFormatBuilder.
 *
 * @see Row
 * @see DriverManager
 */
public class JDBCOutputFormat extends AbstractJDBCOutputFormat<Row> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(JDBCOutputFormat.class);

	private String drivername;
	private final String query;
	private final int batchInterval;
	private final int[] typesArray;
	private List<Row> cacheRows;

	private PreparedStatement upload;
	private int batchCount = 0;

	//metrics
	private transient long outputCnt = 0L;
	private transient long affectedCnt = 0L;

	public JDBCOutputFormat(String username, String password, String drivername,
                            String dbURL, String query, int batchInterval, int[] typesArray) {
		super(username, password, drivername, dbURL);
		this.query = query;
		this.batchInterval = batchInterval;
		this.typesArray = typesArray;
		this.drivername = drivername;
	}

	/**
	 * Connects to the target database and initializes the prepared statement.
	 *
	 * @param taskNumber The number of the parallel instance.
	 * @throws IOException Thrown, if the output could not be opened due to an
	 * I/O problem.
	 */
	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		try {
			Class.forName(drivername);
			establishConnection();
			upload = connection.prepareStatement(query);
			this.cacheRows = new ArrayList<>();
		} catch (SQLException sqe) {
			throw new IllegalArgumentException("open() failed.", sqe);
		} catch (ClassNotFoundException cnfe) {
			throw new IllegalArgumentException("JDBC driver class not found.", cnfe);
		}

		//metrics
		MetricGroup metricGroup = getRuntimeContext().getMetricGroup().addGroup("custom_group");
		metricGroup.gauge("outputCnt", () -> outputCnt);
		metricGroup.gauge("affectedCnt", () -> affectedCnt);

	}

	@Override
	public void writeRecord(Row row) throws IOException {
		LOG.debug("write record {}", row);
		cacheRows.add(row);
		batchCount++;

		if (batchCount >= batchInterval) {
			// execute batch
			LOG.debug("Flush......");
			flush();
		}
	}

	void flush() {
		try {
			if (!connection.isValid(5)) {
				LOG.warn("DB connection timeout.");
				establishConnection();
				upload = connection.prepareStatement(query);
			}
            for (Row row : cacheRows) {
                setRecordToStatement(upload, typesArray, row);
                upload.addBatch();
            }
			outputCnt += cacheRows.size();
			int[] batch = upload.executeBatch();
			int affectedRow = Arrays.stream(batch).sum();
			affectedCnt += affectedRow;
			LOG.debug("JDBC batch flush, affected rows is {}", affectedRow);
			cacheRows.clear();
			batchCount = 0;
		} catch (SQLException e) {
			throw new RuntimeException("Execution of JDBC statement failed.", e);
		}
	}

	/**
	 * Executes prepared statement and closes all resources of this instance.
	 *
	 * @throws IOException Thrown, if the input could not be closed properly.
	 */
	@Override
	public void close() throws IOException {
		if (upload != null) {
			try {
				upload.close();
			} catch (SQLException e) {
				LOG.info("JDBC statement could not be closed: " + e.getMessage());
			} finally {
				upload = null;
			}
		}

		closeDbConnection();
	}

	public void commit() throws SQLException {
		connection.commit();
	}

	public static JDBCOutputFormatBuilder buildJDBCOutputFormat() {
		return new JDBCOutputFormatBuilder();
	}

	/**
	 * Builder for a {@link JDBCOutputFormat}.
	 */
	public static class JDBCOutputFormatBuilder {
		private String username;
		private String password;
		private String drivername;
		private String dbURL;
		private String query;
		private int batchInterval = DEFAULT_FLUSH_MAX_SIZE;
		private int[] typesArray;

		protected JDBCOutputFormatBuilder() {}

		public JDBCOutputFormatBuilder setUsername(String username) {
			this.username = username;
			return this;
		}

		public JDBCOutputFormatBuilder setPassword(String password) {
			this.password = password;
			return this;
		}

		public JDBCOutputFormatBuilder setDrivername(String drivername) {
			this.drivername = drivername;
			return this;
		}

		public JDBCOutputFormatBuilder setDBUrl(String dbURL) {
			this.dbURL = dbURL;
			return this;
		}

		public JDBCOutputFormatBuilder setQuery(String query) {
			this.query = query;
			return this;
		}

		public JDBCOutputFormatBuilder setBatchInterval(int batchInterval) {
			this.batchInterval = batchInterval;
			return this;
		}

		public JDBCOutputFormatBuilder setSqlTypes(int[] typesArray) {
			this.typesArray = typesArray;
			return this;
		}

		/**
		 * Finalizes the configuration and checks validity.
		 *
		 * @return Configured JDBCOutputFormat
		 */
		public JDBCOutputFormat finish() {
			if (this.username == null) {
				LOG.info("Username was not supplied.");
			}
			if (this.password == null) {
				LOG.info("Password was not supplied.");
			}
			if (this.dbURL == null) {
				throw new IllegalArgumentException("No database URL supplied.");
			}
			if (this.query == null) {
				throw new IllegalArgumentException("No query supplied.");
			}
			if (this.drivername == null) {
				throw new IllegalArgumentException("No driver supplied.");
			}

			return new JDBCOutputFormat(
					username, password, drivername, dbURL,
					query, batchInterval, typesArray);
		}
	}

}
