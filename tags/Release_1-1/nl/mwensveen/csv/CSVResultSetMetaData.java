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
package nl.mwensveen.csv;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * MetaData for the CSVResultSet.
 * @author Micha Wensveen
 */
public class CSVResultSetMetaData implements ResultSetMetaData {

	private static final String columnClassName = String.class.getName();
	private List<String> metaDataList = null;
	/**
	 * @see java.sql.ResultSetMetaData#getCatalogName(int)
	 */
	public String getCatalogName(int arg0) throws SQLException {
		check(arg0);
		return "";
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnClassName(int)
	 */
	public String getColumnClassName(int arg0) throws SQLException {
		check(arg0);
		return columnClassName;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnCount()
	 */
	public int getColumnCount() throws SQLException {
		check();
		return metaDataList.size();
	}


	/**
	 * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
	 */
	public int getColumnDisplaySize(int arg0) throws SQLException {
		check(arg0);
		return 0;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnLabel(int)
	 */
	public String getColumnLabel(int arg0) throws SQLException {
		check(arg0);
		return metaDataList.get(arg0-1);
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnName(int)
	 */
	public String getColumnName(int arg0) throws SQLException {
		check(arg0);
		return metaDataList.get(arg0-1);
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnType(int)
	 */
	public int getColumnType(int arg0) throws SQLException {
		check(arg0);
		return Types.VARCHAR;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
	 */
	public String getColumnTypeName(int arg0) throws SQLException {
		check(arg0);
		return columnClassName;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getPrecision(int)
	 */
	public int getPrecision(int arg0) throws SQLException {
		check(arg0);
		return 0;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getScale(int)
	 */
	public int getScale(int arg0) throws SQLException {
		check(arg0);
		return 0;
	}

	/**
	 * @see java.sql.ResultSetMetaData#getSchemaName(int)
	 */
	public String getSchemaName(int arg0) throws SQLException {
		check(arg0);
		return "";
	}

	/**
	 * @see java.sql.ResultSetMetaData#getTableName(int)
	 */
	public String getTableName(int arg0) throws SQLException {
		check(arg0);
		return "";
	}

	/**
	 * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
	 */
	public boolean isAutoIncrement(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
	 */
	public boolean isCaseSensitive(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isCurrency(int)
	 */
	public boolean isCurrency(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
	 */
	public boolean isDefinitelyWritable(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isNullable(int)
	 */
	public int isNullable(int arg0) throws SQLException {
		check(arg0);
		return ResultSetMetaData.columnNullableUnknown;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isReadOnly(int)
	 */
	public boolean isReadOnly(int arg0) throws SQLException {
		check(arg0);
		return true;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isSearchable(int)
	 */
	public boolean isSearchable(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isSigned(int)
	 */
	public boolean isSigned(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.ResultSetMetaData#isWritable(int)
	 */
	public boolean isWritable(int arg0) throws SQLException {
		check(arg0);
		return false;
	}

	/**
	 * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		check();
		return false;
	}

	/**
	 * @see java.sql.Wrapper#unwrap(java.lang.Class)
	 */
	public <T> T unwrap(Class<T> iface) throws SQLException {
		check();
		throw new SQLException("The ResultSetMetaData does not support this method");
	}

	/**
	 * Check if a method can be called on this MetaData.
	 * @throws SQLException
	 */
	private void check() throws SQLException {
		if (metaDataList==null || metaDataList.size()==0) {
			throw new SQLException("No metaData defined");
		}
	}

	/**
	 * Check if a method can be called on the given column index.
	 * @param index int
	 * @throws SQLException
	 */
	private void check(int index) throws SQLException {
		check();
		if (index>metaDataList.size())
			throw new SQLException("Invalid column index (" + index + ")");
		}

	/**
	 * @return the metaData
	 */
	List<String> getMetaDataList() {
		return metaDataList;
	}

	/**
	 * @param metaData the metaData to set
	 */
	void setMetaDataList(List<String> metaDataList) {
		this.metaDataList = metaDataList;
	}
}
