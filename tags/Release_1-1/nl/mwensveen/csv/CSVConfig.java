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

import java.util.Map;

/**
 * This class holds the configuration for the CSVParser and CSVResultSet.
 * 
 * @author Micha Wensveen
 */
public class CSVConfig {
	private Map<Integer, String> columnNames = null;
	private String datePattern = "yyyyMMdd";
	private char decimalPoint = '.';
	private char seperator = ',';
	private boolean startWithMetaDataRow;

	/**
	 * @return the columnNames
	 */
	public Map<Integer, String> getColumnNames() {
		return columnNames;
	}

	/**
	 * Names of the column in the CSV file. This Map can be used if the cvs file has no metadata or 
	 * if (one of) the columnnames need(s) to be overriden.
	 * Note that the key of the map (Integer) is the number of the column, where the first column is 1.
	 * 
	 * @param columnNames the columnNames to set
	 */
	public void setColumnNames(Map<Integer, String> columnNames) {
		this.columnNames = columnNames;
	}

	/**
	 * Pattern that is used when a date is retrieved from the ResultSet. 
	 * Note, this can be changed before the date is retrieved from the resultSet. I.e. everytime a date
	 * is retrieved, a different pattern can be used.
	 * Default is "yyyyMMdd"
	 * @param datePattern the datePattern to set
	 */
	public void setDatePattern(String datePattern) {
		this.datePattern = datePattern;
	}

	/**
	 * Decimal point used in numbers.
	 * Default is point.
	 * @param decimalPoint the decimalPoint to set
	 */
	public void setDecimalPoint(char decimalPoint) {
		this.decimalPoint = decimalPoint;
	}

	/**
	 * Seperator used when the CSV-file is parsed.
	 * Default is comma.
	 * @param seperator the seperator to set
	 */
	public void setSeperator(char seperator) {
		this.seperator = seperator;
	}

	/**
	 * Indiator that tells the parser whether the csv file starts with metadata (= columnNames) or not.
	 * @param startWithMetaDataRow the startWithMetaDataRow to set
	 */
	public void setStartWithMetaDataRow(boolean startWithMetaDataRow) {
		this.startWithMetaDataRow = startWithMetaDataRow;
	}

	/**
	 * Pattern that is used when a date is retrieved from the ResultSet.
	 * @return the datePattern
	 */
	 String getDatePattern() {
		return datePattern;
	}

	/**
	 * @return the decimalPoint used in numbers.
	 */
	char getDecimalPoint() {
		return decimalPoint;
	}

	/**
	 * Seperator used when the CSV-file is parsed.
	 * @return the seperator
	 */
	char getSeperator() {
		return seperator;
	}

	/**
	 * @return the startWithMetaDataRow
	 */
	boolean isStartWithMetaDataRow() {
		return startWithMetaDataRow;
	}

}
