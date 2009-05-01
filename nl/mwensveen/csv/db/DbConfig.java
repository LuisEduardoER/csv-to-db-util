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

import java.util.HashMap;
import java.util.Map;

import nl.mwensveen.csv.db.type.api.DbType;

/**
 * This class holds the configuration used by the DbCreationUtil.
 * 
 * @author Micha Wensveen.
 */
public class DbConfig {
	private final static String DATABASENAME_DEFAULT = "CSVdb";
	private final static String TABLENAME_DEFAULT = "csvTable";
	private boolean createTable;
	private String dataBaseName;
	private Map<String, DbType> dataTypes;
	private DbType extraColumn;
	private String extraColumnName;
	private String jdbcUrl = "";
	private String tableName;
	private DbConnectionManager dbConnectionManager;
	private boolean usePreparedStatement;
	/**
	 * @return the extraColumn
	 */
	public DbType getExtraColumn() {
		return extraColumn;
	}

	/**
	 * @return the jdbcUrl used in the DbCreationUtil.
	 */
	public String getJdbcUrl() {
		return jdbcUrl;
	}



	/**
	 * Boolean to indicate if the table needs te be created.
	 * @param createTable the createTable to set
	 */
	public void setCreateTable(boolean createTable) {
		this.createTable = createTable;
	}

	/**
	 * The name of the database the DbCreationUtil create when using the default jdbcUrl (derby).
	 * When another jdbcUrl is used (eg connect to Oracle), the database name will be in that jdbcUrl and 
	 * this variable is ignored. 
	 * Default = CSVdb
	 * @param dataBaseName the dataBaseName (String)to set
	 */
	public void setDataBaseName(String dataBaseName) {
		if (dataBaseName != null) {
			this.dataBaseName = dataBaseName.trim();
		} else {
			dataBaseName = null;
		}
	}

	/**
	 * This map holds the datatypes that will be assigned to the column in the table 
	 * and used to when inserting rows. 
	 * The key can be:
	 *  1) the name of the field in the CSVResultSet or 
	 *  2) the column number (starting with column 1). 
	 * If for a column neither name or columnnumber is found in the map, the DbCreationUtility will create
	 * the column as LONGVARCHAR.
	 * @param dataTypes the dataTypes (Map<String,DbType>)to set.
	 */
	public void setDataTypes(Map<String, DbType> dataTypes) {
		this.dataTypes = dataTypes;
	}

	/**
	 * The extraColumn holds a DbType that is used to insert a column that is not in the resultset. 
	 * It is typically used to create a primairykey. 
	 * Leave <code>null</code> if not used.
	 * @see nl.mwensveen.csv.db.type.SequentialPrimaryKey.
	 * @param extraColumn the extraColumn to set
	 */
	public void setExtraColumn(DbType extraColumn) {
		this.extraColumn = extraColumn;
	}

	/**
	 * String identifying the url to use in the DriverManager.getConnection().
	 * If not given, the utillity will use "jdbc:derby:<databaseName>;create=true". 
	 * Note: the jdbcUrl must be complete (i.e. with jdbc:).
	 * @param jdbcUrl the jdbcUrl (String)to set
	 */
	public void setJdbcUrl(String jdbcUrl) {
		if (jdbcUrl != null) {
			this.jdbcUrl = jdbcUrl.trim();
		} else {
			this.jdbcUrl = jdbcUrl;
		}
	}

	/**
	 * String that indicates the name of the table this utility will create (if createTable==true) and
	 * into which the rows will be inserted.
	 * Default is csvTable.
	 * @param tableName the tableName (String)to set
	 */
	public void setTableName(String tableName) {
		if (tableName != null) {
			this.tableName = tableName.trim();
		} else {
			this.tableName = null;
		}
	}

	/**
	 * Check if all properties were set and when necessary set the default values.
	 */
	void checkProperties() {
		if (tableName == null || tableName.trim().length() == 0) {
			tableName = TABLENAME_DEFAULT;
		}

		if (dataBaseName == null || dataBaseName.length() == 0) {
			dataBaseName = DATABASENAME_DEFAULT;
		}

		if (dataTypes == null) {
			dataTypes = new HashMap<String, DbType>();
		}

		if (jdbcUrl == null || jdbcUrl.trim().equals("")) {
			jdbcUrl = "jdbc:derby:" + dataBaseName + ";create=true";
		}
	}

	/**
	 * @return the dataBaseName
	 */
	String getDataBaseName() {
		return dataBaseName;
	}

	/**
	 * @return the dataTypes
	 */
	public Map<String, DbType> getDataTypes() {
		return dataTypes;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the createTable
	 */
	boolean isCreateTable() {
		return createTable;
	}

	/**
	 * @param dbConnectionManager the dbConnectionManager to set
	 */
	public void setDbConnectionManager(DbConnectionManager dbConnectionManager) {
		this.dbConnectionManager = dbConnectionManager;
	}

	/**
	 * @return the dbConnectionManager
	 */
	public DbConnectionManager getDbConnectionManager() {
		return dbConnectionManager;
	}

	/**
	 * Indicates that the insert statements are done with a prepared statement. 
	 * Otherwise a normal statement is used.
	 * @param usePreparedStatement the usePreparedStatement to set
	 */
	public void setUsePreparedStatement(boolean usePreparedStatement) {
		this.usePreparedStatement = usePreparedStatement;
	}

	/**
	 * @return the usePreparedStatement
	 */
	public boolean isUsePreparedStatement() {
		return usePreparedStatement;
	}

	/**
	 * @param extraColumnName the extraColumnName to set
	 */
	public void setExtraColumnName(String extraColumnName) {
		this.extraColumnName = extraColumnName;
	}

	/**
	 * @return the extraColumnName
	 */
	public String getExtraColumnName() {
		return extraColumnName;
	}

}
