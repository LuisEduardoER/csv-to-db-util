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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * This class is the implementation of the java.sql.ResultSet that is returned by the CSVParser.
 * This class allows applications to use csv-files as if they were the result of a sql-select statement.
 * The ResultSet is not updatable and of type forward only.
 * Some methods are not supported (see javadoc). An SqlException will be thrown when they are called. 
 * @author Micha Wensveen
 */
public class CSVResultSet implements ResultSet {
	// configuration class
	private CSVConfig config = null;
	
	// index in the list! Note: list is 0 based, ResultSet 1 based.
	private int index = -1;
	// result from the parsing process.
	private List<List<String>> result = null;
	// number of lines in the result;
	private int length = 0;
	// metadata from the parsing process.
	private CSVResultSetMetaData metaData = null;
	// current row processed.
	private List<String> curRow = null;
	// see wasNull().
	private boolean wasNullValue = false;
	
	
	/**
	 * Constructor that passes the config.
	 * @param config
	 */
	public CSVResultSet(CSVConfig config) {
		super();
		this.config = config;
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public boolean absolute(int arg0) throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public void afterLast() throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public void beforeFirst() throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public void cancelRowUpdates() throws SQLException {
		throw new SQLException("The result set is not updatable"); 
	}

	/**
	 * @see java.sql.ResultSet#clearWarnings()
	 */
	public void clearWarnings() throws SQLException {
		check();
	}

	/**
	 * @see java.sql.ResultSet#close()
	 */
	public void close() throws SQLException {
		check();
		result = null;
		metaData = null;
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public void deleteRow() throws SQLException {
		throw new SQLException("The result set is not updatable"); 
	}

	/**
	 * @see java.sql.ResultSet#findColumn(java.lang.String)
	 */
	public int findColumn(String arg0) throws SQLException {
		check();
		int i = 1;
		for (String name : metaData.getMetaDataList()) {
			if (name.equals(arg0)) {
				return i;
			}
			i++;
		}
		throw new SQLException("The ResultSet object does not contain columnName " + arg0);
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public boolean first() throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Array getArray(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Array getArray(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public InputStream getAsciiStream(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public InputStream getAsciiStream(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return null;
		}
		// remove groupering and use default '.' as decimal seperator.
		return BigDecimal.valueOf(Double.parseDouble(modifyNumber(value)));
	}

	/**
	 * Convert a number to a format that can be used in the number.parse methods.
	 * I.e. no , as grouping and . as decimalpoint.
	 * @param value
	 * @return
	 */
	private String modifyNumber(String value) {
		StringBuilder sb = new StringBuilder();
		
		String wholeNumber = "";
		int decimalPointIndex = value.indexOf(config.getDecimalPoint());
		if (decimalPointIndex<0) {
			wholeNumber = value;
		} else {
			wholeNumber = value.substring(0, decimalPointIndex);
		}
		
		StringTokenizer st = new StringTokenizer(wholeNumber, ".,");
		while (st.hasMoreElements()) {
			sb.append(st.nextElement());
		}
		
		if (decimalPointIndex>=0 && decimalPointIndex<value.length()-1) {
			sb.append(".");
			sb.append(value.substring(decimalPointIndex+1));
		}


		return sb.toString();
	}

	/**
	 * @see java.sql.ResultSet#getBigDecimal(java.lang.String)
	 */
	public BigDecimal getBigDecimal(String arg0) throws SQLException {
		return getBigDecimal(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public BigDecimal getBigDecimal(int arg0, int arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this depricated method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public BigDecimal getBigDecimal(String arg0, int arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this depricated method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public InputStream getBinaryStream(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public InputStream getBinaryStream(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Blob getBlob(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Blob getBlob(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(int)
	 */
	public boolean getBoolean(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return false;
		}
		return Boolean.parseBoolean(value);
	}

	/**
	 * @see java.sql.ResultSet#getBoolean(java.lang.String)
	 */
	public boolean getBoolean(String arg0) throws SQLException {
		return getBoolean(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getByte(int)
	 */
	public byte getByte(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Byte.parseByte(modifyNumber(value));
		
	}

	/**
	 * @see java.sql.ResultSet#getByte(java.lang.String)
	 */
	public byte getByte(String arg0) throws SQLException {
		return getByte(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public byte[] getBytes(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public byte[] getBytes(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Reader getCharacterStream(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Reader getCharacterStream(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Clob getClob(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException Always throws this one.
	 */
	public Clob getClob(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getConcurrency()
	 */
	public int getConcurrency() throws SQLException {
		return ResultSet.CONCUR_READ_ONLY;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public String getCursorName() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getDate(int)
	 */
	public Date getDate(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return null;
		}
		SimpleDateFormat dateFormat = new SimpleDateFormat(config.getDatePattern());
		try {
			return new Date(dateFormat.parse(value).getTime());
		} catch (ParseException e) {
			throw new SQLException("Error parsing with dateFormat " + dateFormat.toPattern(), e);
		}
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String)
	 */
	public Date getDate(String arg0) throws SQLException {
		return getDate(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getDate(int, java.util.Calendar)
	 */
	public Date getDate(int arg0, Calendar arg1) throws SQLException {
		Date sqlDate = getDate(arg0);
		if (sqlDate==null) {
			return null;
		}
		arg1.setTime(sqlDate);
		return new Date(arg1.getTime().getTime());
	}

	/**
	 * @see java.sql.ResultSet#getDate(java.lang.String, java.util.Calendar)
	 */
	public Date getDate(String arg0, Calendar arg1) throws SQLException {
		return getDate(findColumn(arg0), arg1);
	}

	/**
	 * @see java.sql.ResultSet#getDouble(int)
	 */
	public double getDouble(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Double.parseDouble(modifyNumber(value));
	}

	public double getDouble(String arg0) throws SQLException {
		return getDouble(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException {
		return ResultSet.FETCH_FORWARD;
	}

	/**
	 * @see java.sql.ResultSet#getFetchSize()
	 */
	public int getFetchSize() throws SQLException {
		return  length;
	}

	/**
	 * @see java.sql.ResultSet#getFloat(int)
	 */
	public float getFloat(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Float.parseFloat(modifyNumber(value));
	}

	/**
	 * @see java.sql.ResultSet#getFloat(java.lang.String)
	 */
	public float getFloat(String arg0) throws SQLException {
		return getFloat(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public int getHoldability() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getInt(int)
	 */
	public int getInt(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Integer.parseInt(modifyNumber(value));
	}


	/**
	 * @see java.sql.ResultSet#getInt(java.lang.String)
	 */
	public int getInt(String arg0) throws SQLException {
		return getInt(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getLong(int)
	 */
	public long getLong(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Long.parseLong(modifyNumber(value));
	}

	/**
	 * @see java.sql.ResultSet#getLong(java.lang.String)
	 */
	public long getLong(String arg0) throws SQLException {
		return getLong(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		check();
		return metaData;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Reader getNCharacterStream(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Reader getNCharacterStream(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public NClob getNClob(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public NClob getNClob(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public String getNString(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public String getNString(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getObject(int)
	 */
	public Object getObject(int arg0) throws SQLException {
		return getString(arg0);
	}

	/**
	 * @see java.sql.ResultSet#getObject(java.lang.String)
	 */
	public Object getObject(String arg0) throws SQLException {
		return getObject(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Ref getRef(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Ref getRef(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getRow()
	 */
	public int getRow() throws SQLException {
		return index + 1;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public RowId getRowId(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public RowId getRowId(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public SQLXML getSQLXML(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public SQLXML getSQLXML(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getShort(int)
	 */
	public short getShort(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return 0;
		}
		return Short.parseShort(modifyNumber(value));
	}

	/**
	 * @see java.sql.ResultSet#getShort(java.lang.String)
	 */
	public short getShort(String arg0) throws SQLException {
		return getShort(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public Statement getStatement() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getString(int)
	 */
	public String getString(int arg0) throws SQLException {
		checkRow();
		if (arg0>metaData.getColumnCount()) {
			throw new SQLException("For this resultSet the columnIndex cannot be larger than " + metaData.getColumnCount() + "(" +arg0 + ")"); 
		}
		if (arg0>curRow.size()) {
			wasNullValue = Boolean.TRUE;
			return null;
		}
		String value = curRow.get(arg0-1);
		if (value.equals("")) {
			wasNullValue = Boolean.TRUE;
			return null;
		}
		wasNullValue = Boolean.FALSE;
		return value;
	}

	/**
	 * @see java.sql.ResultSet#getString(java.lang.String)
	 */
	public String getString(String arg0) throws SQLException {
		return getString(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getTime(int)
	 */
	public Time getTime(int arg0) throws SQLException {
		Date sqlDate = getDate(arg0);
		if (sqlDate==null) {
			return null;
		}
		return new Time(sqlDate.getTime());
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String)
	 */
	public Time getTime(String arg0) throws SQLException {
		return getTime(findColumn(arg0));
	}

	/**
	 * @see java.sql.ResultSet#getTime(int, java.util.Calendar)
	 */
	public Time getTime(int arg0, Calendar arg1) throws SQLException {
		Time sqlTime = getTime(arg0);
		if (sqlTime==null) {
			return null;
		}
		arg1.setTime(sqlTime);
		return new Time(arg1.getTime().getTime());
	}

	/**
	 * @see java.sql.ResultSet#getTime(java.lang.String, java.util.Calendar)
	 */
	public Time getTime(String arg0, Calendar arg1) throws SQLException {
		return getTime(findColumn(arg0), arg1);
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int)
	 */
	public Timestamp getTimestamp(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return null;
		}
		return new Timestamp(Long.parseLong(modifyNumber(value)));
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String)
	 */
	public Timestamp getTimestamp(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(int arg0, Calendar arg1) throws SQLException {
		Timestamp sqlTimestamp = getTimestamp(arg0);
		if (sqlTimestamp==null) {
			return null;
		}
		arg1.setTime(sqlTimestamp);
		return new Timestamp(arg1.getTime().getTime());
	}

	/**
	 * @see java.sql.ResultSet#getTimestamp(java.lang.String, java.util.Calendar)
	 */
	public Timestamp getTimestamp(String arg0, Calendar arg1) throws SQLException {
		return getTimestamp(findColumn(arg0), arg1);
	}

	/**
	 * @see java.sql.ResultSet#getType()
	 */
	public int getType() throws SQLException {
		return ResultSet.TYPE_FORWARD_ONLY;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public URL getURL(int arg0) throws SQLException {
		String value = getString(arg0);
		if (value==null) {
			return null;
		}
		try {
			return new URL(value);
		} catch (MalformedURLException e) {
			throw new SQLException("No valid URL could be created (" + value +")", e);
		}
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public URL getURL(String arg0) throws SQLException {
		return getURL(findColumn(arg0));
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public InputStream getUnicodeStream(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public InputStream getUnicodeStream(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public SQLWarning getWarnings() throws SQLException {
		check();
		return null;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void insertRow() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * @see java.sql.ResultSet#isAfterLast()
	 */
	public boolean isAfterLast() throws SQLException {
		check();
		return index>=result.size();
	}

	/**
	 * @see java.sql.ResultSet#isBeforeFirst()
	 */
	public boolean isBeforeFirst() throws SQLException {
		check();
		return index<0;
	}

	/**
	 * @see java.sql.ResultSet#isClosed()
	 */
	public boolean isClosed() throws SQLException {
		return result!=null;
	}

	/**
	 * @see java.sql.ResultSet#isFirst()
	 */
	public boolean isFirst() throws SQLException {
		check();
		return index==0;
	}

	public boolean isLast() throws SQLException {
		check();
		return index==result.size()-1;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean last() throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void moveToCurrentRow() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void moveToInsertRow() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean next() throws SQLException {
		check();
		index++;
		if (index>=result.size()) {
			index--;
			return false;
		}
		curRow = result.get(index);
		wasNullValue = false;
		return true;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean previous() throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * @see java.sql.ResultSet#refreshRow()
	 */
	public void refreshRow() throws SQLException {
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean relative(int arg0) throws SQLException {
		throw new SQLException("The result set type is TYPE_FORWARD_ONLY"); 
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean rowDeleted() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean rowInserted() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean rowUpdated() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void setFetchDirection(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void setFetchSize(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateArray(int arg0, Array arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateArray(String arg0, Array arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateAsciiStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBigDecimal(int arg0, BigDecimal arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBigDecimal(String arg0, BigDecimal arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(int arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(String arg0, InputStream arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBinaryStream(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(int arg0, Blob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(String arg0, Blob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(int arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(String arg0, InputStream arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(int arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBlob(String arg0, InputStream arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBoolean(int arg0, boolean arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBoolean(String arg0, boolean arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateByte(int arg0, byte arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateByte(String arg0, byte arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBytes(int arg0, byte[] arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateBytes(String arg0, byte[] arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(int arg0, Reader arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(String arg0, Reader arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(int arg0, Clob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(String arg0, Clob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateDate(int arg0, Date arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateDate(String arg0, Date arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateDouble(int arg0, double arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateDouble(String arg0, double arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateFloat(int arg0, float arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateFloat(String arg0, float arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateInt(int arg0, int arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateInt(String arg0, int arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateLong(int arg0, long arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateLong(String arg0, long arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNCharacterStream(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNCharacterStream(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNCharacterStream(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNCharacterStream(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(int arg0, NClob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(String arg0, NClob arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(int arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(String arg0, Reader arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(int arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNClob(String arg0, Reader arg1, long arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNString(int arg0, String arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNString(String arg0, String arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNull(int arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateNull(String arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateObject(int arg0, Object arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateObject(String arg0, Object arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateObject(int arg0, Object arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateObject(String arg0, Object arg1, int arg2) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateRef(int arg0, Ref arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateRef(String arg0, Ref arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateRow() throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateRowId(int arg0, RowId arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateRowId(String arg0, RowId arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateSQLXML(int arg0, SQLXML arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");

	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateSQLXML(String arg0, SQLXML arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateShort(int arg0, short arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateShort(String arg0, short arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateString(int arg0, String arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateString(String arg0, String arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateTime(int arg0, Time arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateTime(String arg0, Time arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateTimestamp(int arg0, Timestamp arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public void updateTimestamp(String arg0, Timestamp arg1) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	public boolean wasNull() throws SQLException {
		return wasNullValue;
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Method not supported.
	 * @throws SQLException. Always throws this one.
	 */
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw new SQLException("The ResultSet does not support this method");
	}

	/**
	 * Check if any action on the resultset can be done.
	 * @throws SQLException
	 */
	private void check() throws SQLException {
		if (result == null) {
			throw new SQLException("ResultSet is closed");
		}
	}
	/**
	 * Check if any action on the curRow of the resultset can be done.
	 * @throws SQLException
	 */
	private void checkRow() throws SQLException {
		check();
		if (curRow==null) {
			throw new SQLException("No current row selected (either before first or after last)");			
		}
	}

	/**
	 * @param result the result to set
	 */
	void setResult(List<List<String>> result) {
		this.result = result;
		length=result.size();
	}

	/**
	 * Set the metadata for this ResultSet.
	 * @param metaData the metaData to set
	 */
	private void setMetaData(List<String> metaDataList) {
		metaData = new CSVResultSetMetaData();
		metaData.setMetaDataList(metaDataList);
	}
	/**
	 * Create the metaData. 
	 * Check if the CSVConfig has names that need to override the metaData from the file.
	 * If the CSV-file does not start with metaDate, create metaData with defaults.
	 * Based on the maximum number of columns found, create the columnnames as "Field1", "Field2", etc.
	 * @param size int with the number of columns.
	 */
	void createMetaData(List<String> metaDataList, int size) {
		if (metaDataList==null) {
			metaDataList = new ArrayList<String>();
		}
		fillMetaData(metaDataList, size);
		setMetaData(metaDataList);
	}
	
	private void fillMetaData(List<String> md, int size) {
		NumberFormat nf = NumberFormat.getIntegerInstance();
		nf.setMinimumIntegerDigits(3);
		nf.setMaximumIntegerDigits(3);
		nf.setGroupingUsed(false);

		Map<Integer, String> colNames = config.getColumnNames();
		if (colNames==null) {
			colNames = new HashMap<Integer, String>();
		}
		for (int i = 1; i <= size; i++) {
			if (md.size()<i) { 
				// metadata does not have an entry for every column. Add one.
				md.add(getColumnName(i, null, colNames, nf));
			} else {
				md.set(i-1, getColumnName(i, md.get(i-1), colNames, nf));
			}
		}
	}

	private String getColumnName(int i, String mdName, Map<Integer, String> colNames, NumberFormat nf) {
		String name = null;
		if (colNames.containsKey(Integer.valueOf(i))) {
			name = colNames.get(Integer.valueOf(i));
		} else if (mdName==null) {
			name = "Field"+nf.format(i);
		} else {
			name = mdName;
		}
		name = name.replace(' ', '_');
		return name;
	}

	/**
	 * Method called after inserting the result set to the db and before returning it to the user.
	 */
	void resetIndex() {
		index = -1;
	}
}
