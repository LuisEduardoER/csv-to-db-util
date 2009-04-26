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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import nl.mwensveen.csv.db.DbConfig;
import nl.mwensveen.csv.db.DbCreationUtil;

import org.apache.log4j.Logger;


/**
 * Parse Comma-separated values (CSV) files to java.sql.ResultSet or directly to a Database. 
 * @author Micha Wensveen 
 */
public class CSVParser {
	/** Configuration for parsing */
	private CSVConfig config = null;

	/** DbCreation util used to create a db while parsing */
	private DbCreationUtil dbCreationUtil;

	/** the reader that will be processed */
	private BufferedReader input = null;

	/** line that is being processed */
	private String line = null;

	private Logger log = Logger.getLogger(CSVParser.class);
	private boolean toDb = false;

	/**
	 * Construct a CSV parser, with the default separator (`,') and datePatern("yyyyMMdd"). 
	 * The parser is used to retrieve a ResultSet.
	 */
	public CSVParser() {
		this(new CSVConfig());
	}

	/**
	 * Construct a CSV parser with a Configuration. 
	 * The parser is used to retrieve a ResultSet.
	 * 
	 * @param config the CSVConfig to use.
	 */
	public CSVParser(CSVConfig config) {
		this.config = config;
	}

	/**
	 * Construct a CSV parser with a Configuration for the parser and DbCreationUtil. 
	 * The parser is used to retrieve a ResultSet and store it in a DataBase.
	 * 
	 * @param config the CSVConfig to use.
	 */
	public CSVParser(CSVConfig config, DbConfig dbConfig) {
		this.config = config;
		this.dbCreationUtil = new DbCreationUtil(dbConfig);
	}

	/**
	 * Parse the text in the inputReader in a list that contains the parsed lines of the csv-file.
	 * 
	 * @param inputReader BufferedReader
	 * @return ResultSet
	 * @throws CSVParserException
	 */
	public ResultSet parse(BufferedReader inputReader) throws CSVParserException {
		CSVResultSet resultSet = new CSVResultSet(config);
		List<List<String>> result = new ArrayList<List<String>>();
		resultSet.setResult(result);

		boolean metaDataRow = config.isStartWithMetaDataRow();
		boolean processedMetaData = false;
		List<String> metaData = null;

		input = inputReader;
		try {
			line = input.readLine();
		} catch (IOException e) {
			log.error("Error reading file", e);
			throw new CSVParserException("Error reading file", e);
		}
		int maxColumns = 0;

		while (line != null) {
			// parse the line to a list.
			List<String> parsedLine = parseLine();
			// keep track of the maximum of columns found in the file.
			if (parsedLine.size() > maxColumns) {
				maxColumns = parsedLine.size();
			}
			// Process this line as metaData or as normal data.
			if (metaDataRow) {
				metaData = parsedLine;
			} else {
				result.add(parsedLine);
			}

			if (toDb) {
				// first time here, there is no resultset.
				if (!processedMetaData) {
					int columns = maxColumns;
					if (config.getColumnNames() != null) {
						columns = Math.max(config.getColumnNames().size(), columns);
					}
					resultSet.createMetaData(metaData, columns);
					try {
						dbCreationUtil.init(resultSet.getMetaData());
					} catch (SQLException e) {
						log.error("Cannot initialize DataBase", e);
						throw new CSVParserException("Cannot initialize DataBase", e);
					}
					processedMetaData = true;
				}
				// just processed the metaData, nothing to do
				if (!metaDataRow) {
					try {
						dbCreationUtil.processResultSet(resultSet);
					} catch (SQLException e) {
						log.error("Error creating row in DB", e);
						throw new CSVParserException("Error creating row in DB", e);
					}
				}
			}
			metaDataRow = false;

			// paresLine() can also perform a readLine, so check again.
			if (line != null) {
				try {
					line = input.readLine();
				} catch (IOException e) {
					log.error("Error reading file", e);
					throw new CSVParserException("Error reading file", e);
				}
			}
		}
		if (!toDb) {
			resultSet.createMetaData(metaData, maxColumns);
		}
		resultSet.resetIndex();
		return resultSet;
	}

	/**
	 * Parse the text in the inputReader in a ResultSet that contains the parsed lines of the csv-file.
	 * 
	 * @param inputReader Reader
	 * @throws CSVParserException
	 */
	public ResultSet parse(Reader inputReader) throws CSVParserException {
		return parse(new BufferedReader(inputReader));
	}

	/**
	 * Parse the text in the inputReader in a list that contains the parsed lines of the csv-file and put the result in a database.
	 * 
	 * @param inputReader BufferedReader
	 * @throws CSVParserException
	 */
	public ResultSet parseToDb(BufferedReader inputReader) throws CSVParserException {
		// When parsing to the DB directly, we need to have metadata.
		// Either from the csv file or from the Config
		if (!config.isStartWithMetaDataRow() && (config.getColumnNames() == null || config.getColumnNames().size() == 0)) {
			throw new CSVParserException("No metadata defined");
		}
		if (dbCreationUtil == null) {
			throw new CSVParserException("No DbConfig defined");
		}

		toDb = true;
		CSVResultSet resultSet = (CSVResultSet) parse(inputReader);
		return resultSet;
	}

	/**
	 * Parse the text in the inputReader in a ResultSet that contains the parsed lines of the csv-file and put the result in a database.
	 * 
	 * @param inputReader Reader
	 * @return ResultSet
	 * @throws CSVParserException
	 */
	public ResultSet parseToDb(Reader inputReader) throws CSVParserException {
		return parseToDb(new BufferedReader(inputReader));
	}

	/**
	 * Parse the text in the File in a ResultSet that contains the parsed lines of the csv-file and put the result in a database.
	 * 
	 * @param fileName String that is the fully qualified path to the file to be processed.
	 * @return ResultSet
	 * @throws CSVParserException
	 */
	public ResultSet parseToDb(String fileName) throws CSVParserException {
		try {
			return parseToDb(new BufferedReader(new FileReader(fileName)));
		} catch (FileNotFoundException e) {
			log.error(e);
			throw new CSVParserException(e);
		}
	}

	/**
	 * Get a field that is not surrounded by quotes.
	 * 
	 * @param sb Stringbuffer with the field value
	 * @param i int with the start index of the field in the this.line
	 * @return int the index of the next seperator
	 */

	private int getNormalField(StringBuffer sb, int i) {
		// look for the next separator
		int j = line.indexOf(config.getSeperator(), i);
		if (j == -1) {
			// no seperator found. Field is to end of line.
			sb.append(line.substring(i));
			return line.length();
		} else {
			sb.append(line.substring(i, j));
			return j;
		}
	}

	/**
	 * Get a field that is surrounding by quotes.
	 * 
	 * @param sb Stringbuffer with the field value
	 * @param index int with the start index of the field in the this.line (after the quote)	
	 * @return int the index of the next seperator
	 * @throws CSVParserException
	 */
	private int getQuotedField(StringBuffer sb, int i) throws CSVParserException {
		int len = line.length();
		int lastIndex = len - 1;

		int j = line.indexOf('"', i);
		while (true) {
			// The quote is the last on the line or followed by a seperator. Found end of the field.
			if (j==lastIndex || line.charAt(j+1)==config.getSeperator()) {
				sb.append(line.substring(i, j));
				break;
			}
			// The quote is followd by another qoute. This means an 'escaped quote'. 
			// put one quote in the field.
			if (line.charAt(j+1)=='"') {
				sb.append(line.substring(i, j+1));
				i = j+2;
			}
			// no quote found, or i was just set to a char on the next line, concatenate next line.
			if (j==-1 || i>lastIndex) {
				String newLine;
				try {
					newLine = input.readLine();
				} catch (IOException e) {
					log.error(e);
					throw new CSVParserException(e);
				}
				if (newLine != null) {
					line = line + "\n" + newLine;
					len = line.length();
					lastIndex = len - 1;
				}
			}
			// find next index of the quote.
			j = line.indexOf('"', i);
		}

		// done with this field. Return the index of the seperator.
		return j+1;
	}

	/**
	 * parse: break the <code>this.list</code> into fields
	 * 
	 * @return List<String> containing each field as an element.
	 * @throws CSVParserException
	 */
	private List<String> parseLine() throws CSVParserException {
		List<String> list = new ArrayList<String>(); // hold the seperate values of this line.

		// empty line.
		if (line.length() == 0) {
			list.add(line);
			return list;
		}

		int index = 0;
		while (index < line.length()) {
			StringBuffer sb = new StringBuffer();
			if (line.charAt(index) == '"') {
				index = getQuotedField(sb, ++index); // skip quote
			} else {
				index = getNormalField(sb, index);
			}
			list.add(sb.toString());
			index++;
		}

		return list;
	}
}