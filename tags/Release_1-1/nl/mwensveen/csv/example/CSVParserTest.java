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
package nl.mwensveen.csv.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import nl.mwensveen.csv.CSVConfig;
import nl.mwensveen.csv.CSVParser;
import nl.mwensveen.csv.CSVParserException;
import nl.mwensveen.csv.db.DbConfig;
import nl.mwensveen.csv.db.DbCreationUtil;
import nl.mwensveen.csv.db.type.BigIntDbType;
import nl.mwensveen.csv.db.type.CharDbType;
import nl.mwensveen.csv.db.type.DateDbType;
import nl.mwensveen.csv.db.type.DecimalDbType;
import nl.mwensveen.csv.db.type.DoubleDbType;
import nl.mwensveen.csv.db.type.FloatDbType;
import nl.mwensveen.csv.db.type.IntegerDbType;
import nl.mwensveen.csv.db.type.LongVarcharDbType;
import nl.mwensveen.csv.db.type.NumericDbType;
import nl.mwensveen.csv.db.type.RealDbType;
import nl.mwensveen.csv.db.type.SequentialPrimaryKey;
import nl.mwensveen.csv.db.type.SmallIntDbType;
import nl.mwensveen.csv.db.type.TimeDbType;
import nl.mwensveen.csv.db.type.TimestampDbType;
import nl.mwensveen.csv.db.type.VarcharDbType;
import nl.mwensveen.csv.db.type.api.DbType;

/**
 * Show how the CsvParser and DbCreationUtil can be used.
 * @author mwensveen
 *
 */
public class CSVParserTest {

	public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException, CSVParserException {
		CSVParser csvParser = new CSVParser(createCSVConfig());
		processRS(csvParser, new BufferedReader(new FileReader(args[0])));
		
		DbConfig dbConfig = createDBConfig("MyDB");
		csvParser = new CSVParser(createCSVConfig(), dbConfig);
		processDB(csvParser, new BufferedReader(new FileReader(args[0])));
		System.out.println(dbConfig.getJdbcUrl());
		
//		csvParser = new CSVParser(createCSVConfig());
//		processRSDB(csvParser, new BufferedReader(new FileReader(args[0])));
	}

	private static DbConfig createDBConfig(String dbName) {
		DbConfig config = new DbConfig();
		Map<String, DbType> dbTypes = new HashMap<String, DbType>();
		config.setDataTypes(dbTypes);
		// Setup the datetypes for (some) columns, by column-name or index (starting with 1).
		dbTypes.put("1", new BigIntDbType() );
		dbTypes.put("2", new CharDbType() );
		dbTypes.put("3", new DateDbType());
		dbTypes.put("4", new DecimalDbType());
		dbTypes.put("5", new DoubleDbType());
		dbTypes.put("6", new FloatDbType());
		dbTypes.put("7", new IntegerDbType());
		dbTypes.put("8", new LongVarcharDbType());
		dbTypes.put("9", new NumericDbType());
		dbTypes.put("10", new RealDbType());
		dbTypes.put("11", new SmallIntDbType());
		dbTypes.put("12", new TimeDbType());
		dbTypes.put("13", new TimestampDbType());
		dbTypes.put("14", new VarcharDbType());
		config.setCreateTable(false);
		config.setExtraColumn(new SequentialPrimaryKey(40));
		config.setExtraColumnName("RowNumber");
		config.setTableName("Data");
		config.setDataBaseName(dbName);
		config.setUsePreparedStatement(true);
		// Specify a jdbc connection url, or use the default derby.
		//  config.setJdbcUrl("jdbc:oracle:.....")
		return config;
	}

	private static CSVConfig createCSVConfig() {
		// Construct a new CSV parser with a non-default config.
		CSVConfig config = new CSVConfig();
		// Dutch style, use a ',' as decimal point
		config.setDecimalPoint(',');
		// define a seperator if it's not ','. Eg tab.
		config.setSeperator(';');
		// define a dateFormat is needed.
		config.setDatePattern("yyyyMMdd");
		config.setStartWithMetaDataRow(true);
		
		// Override some columnNames. Because the name in the metadata is invalid.
		Map<Integer, String> colNames = new HashMap<Integer, String>();
		config.setColumnNames(colNames);
		colNames.put(1, "StartDate");
		return config;
	}

	/**
	 * Process a Reader to a ResultSet.
	 * @param csv
	 * @param is
	 * @throws CSVParserException 
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	private static void processRS(CSVParser csv, BufferedReader is) throws CSVParserException, SQLException, ClassNotFoundException {
		System.out.println("Start processRS: " + new Date());
		// Get the resultset from the database.
		ResultSet rs = csv.parse(is);
		System.out.println("End processRS: " + new Date());

		ResultSetMetaData md = rs.getMetaData();
		while (rs.next()) {
			for (int i =1; i<= md.getColumnCount(); i++) {
				System.out.println(md.getColumnName(i) + " = " + rs.getString(i));
			}
		}
	}
	/**
	 * Process a Reader to a DB.
	 * @param csv
	 * @param is
	 * @throws CSVParserException 
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	private static void processDB(CSVParser csv, BufferedReader is) throws CSVParserException, SQLException, ClassNotFoundException {
		System.out.println("Start processDB: " + new Date());
		@SuppressWarnings("unused")
		ResultSet rs = csv.parseToDb(is);
		System.out.println("End processDB: " + new Date());
	}
	/**
	 * Process a Reader to a ResultSet and that one into a DB.
	 * @param csv
	 * @param is
	 * @throws CSVParserException 
	 * @throws ClassNotFoundException 
	 * @throws SQLException 
	 */
	private static void processRSDB(CSVParser csv, BufferedReader is) throws CSVParserException, SQLException, ClassNotFoundException {
		// Get the resultset from the database.
		System.out.println("Start processRSDB: " + new Date());
		ResultSet rs = csv.parse(is);
 
		DbConfig config = createDBConfig("AnotherDB");
		DbCreationUtil dbc = new DbCreationUtil(config);
		String jdbcUrl = dbc.createDB(rs);
		System.out.println("End processRSDB: " + new Date());
		System.out.println(jdbcUrl);
	}
}
