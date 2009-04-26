/*
 * Copyright (c) 2008, Micha Wensveen (mwensveen.nl)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 * 	  this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright notice,
 * 	  this list of conditions and the following disclaimer in the documentation
 * 	  and/or other materials provided with the distribution.
 *     * Neither the name of mwensveen.nl nor the names of its contributors may be
 * 	  used to endorse or promote products derived from this software without
 * 	  specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package nl.mwensveen.csv.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import nl.mwensveen.csv.db.type.LongVarcharDbType;
import nl.mwensveen.csv.db.type.api.DbType;

/**
 * This utility will put the ResultSet (of the CSVParser) into a database. This way, the CSV file cannot only be processed once in a sequential order,
 * but can also be used to query. Default DB is the Apache Derby 10.4.1.3.
 * 
 * @author Micha Wensveen.
 */
public class DbCreationUtil {
	private DbConfig config;
	private Connection connection;
	private Statement st;

	public DbCreationUtil() {
		this(new DbConfig());
	}

	public DbCreationUtil(DbConfig config) {
		super();
		this.config = config;
	}

	/**
	 * Create the database for the given resultset
	 * 
	 * @param resultset CSVResultSet from the CSVParser.
	 * @return The jdbcUrl that was used to get the connection from the DriverManager.
	 * @throws ClassNotFoundException
	 * @throws SQLException.
	 */
	public String createDB(ResultSet resultset) throws SQLException, ClassNotFoundException {
		try {
			init(resultset.getMetaData());
			processResultSet(resultset);
			while (resultset.next()) {
				String insertStatement = makeInsertStatement(resultset);
				st.execute(insertStatement);
			}
		} finally {
			close();
		}
		return config.getJdbcUrl();
	}

	/**
	 * Process all records in the ResultSet to rows in the DB.
	 * @param resultset
	 * @throws SQLException
	 */
	public void processResultSet(ResultSet resultset) throws SQLException {
		while (resultset.next()) {
			String insertStatement = makeInsertStatement(resultset);
			st.execute(insertStatement);
		}
	}

	/**
	 * Initialise the connection and create the table is necessary.
	 * 
	 * @param resultSetMetaData
	 * @throws SQLException
	 */
	public void init(ResultSetMetaData resultSetMetaData) throws SQLException {
		config.checkProperties();
		connection = DriverManager.getConnection(config.getJdbcUrl());
		st = connection.createStatement();

		if (config.isCreateTable()) {
			String creatTableStatement = makeCreateTableStatement(resultSetMetaData);
			st.execute(creatTableStatement);
		}
	}

	/**
	 * close the connection.
	 * 
	 * @throws SQLException
	 */
	public void close() throws SQLException {
		if (st != null && !st.isClosed()) {
			st.close();
		}
		if (connection != null && !connection.isClosed()) {
			connection.close();
		}
	}

	/**
	 * Create the statement that will insert one row into the table.
	 * 
	 * @param resultset
	 * @return
	 * @throws SQLException
	 */
	private String makeInsertStatement(ResultSet resultset) throws SQLException {
		StringBuilder is = new StringBuilder();
		is.append("insert into ");
		is.append(config.getTableName());
		is.append(" values (");

		boolean firstColumn = true;
		if (config.getExtraColumn() != null) {
			is.append(config.getExtraColumn().getInsertValue(0, resultset));
			firstColumn = false;
		}

		ResultSetMetaData metadata = resultset.getMetaData();
		int count = metadata.getColumnCount();
		for (int i = 1; i <= count; i++) {
			if (!firstColumn) {
				is.append(", ");
			} else {
				firstColumn = false;
			}
			DbType dataType = getDataType(i, metadata);
			String cellValue = dataType.getInsertValue(i, resultset);
			is.append(cellValue);
		}
		is.append(")");
		return is.toString();
	}

	/**
	 * Create the statement that will create the database Table.
	 * 
	 * @param metaData ResultSetMetaData
	 * @return String in the form CREATE TABLE table-Name( {Simple-column-Name DataType[ , {Simple-column-Name DataType ] * )
	 * @throws SQLException
	 */
	private String makeCreateTableStatement(ResultSetMetaData metaData) throws SQLException {
		// create table statement
		StringBuilder ct = new StringBuilder();
		ct.append("CREATE TABLE ");
		ct.append(config.getTableName());
		ct.append("(");
		boolean firstColumn = true;
		if (config.getExtraColumn() != null) {
			ct.append(config.getExtraColumn().getSqlType());
			firstColumn = false;
		}
		//		ResultSetMetaData metaData = resultset.getMetaData();
		for (int i = 1; i <= metaData.getColumnCount(); i++) {
			if (!firstColumn) {
				ct.append(", ");
			} else {
				firstColumn = false;
			}
			String colName = getColumnName(metaData, i);
			DbType dataType = getDataType(i, metaData);
			ct.append(colName);
			ct.append(" ");
			ct.append(dataType.getSqlType());
		}
		ct.append(")");
		return ct.toString();
	}

	/**
	 * Determine the columnName based on the MetaData.
	 * 
	 * @param metaData
	 * @param i
	 * @return String
	 * @throws SQLException
	 */
	private String getColumnName(ResultSetMetaData metaData, int i) throws SQLException {
		String name = metaData.getColumnName(i);
		name = name.replace(' ', '_');
		return name;
	}

	/**
	 * Get the DataType to use based on the index i.
	 * 
	 * @param i
	 * @param metaData
	 * @return
	 * @throws SQLException
	 */
	private DbType getDataType(int i, ResultSetMetaData metaData) throws SQLException {
		String colName = metaData.getColumnName(i);
		DbType dataType = config.getDataTypes().get(colName);
		if (dataType == null) {
			dataType = config.getDataTypes().get(Integer.toString(i));
			if (dataType == null) {
				dataType = new LongVarcharDbType();
				;
			}
		}
		return dataType;
	}

	/**
	 * @param config the config to set
	 */
	public void setConfig(DbConfig config) {
		this.config = config;
	}

	/**
	 * @return the config
	 */
	public DbConfig getConfig() {
		return config;
	}

}
