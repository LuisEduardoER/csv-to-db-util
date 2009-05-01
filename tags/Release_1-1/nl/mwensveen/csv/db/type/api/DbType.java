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
package nl.mwensveen.csv.db.type.api;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Interface that must be used by all types that can be passed to the DbCreationUtil.
 * 
 * @author mwensveen
 * 
 */
public interface DbType {
	/**
	 * @return the string that identifies this Type in the <code>create table</code> statement.
	 */
	String getSqlType();

	/**
	 * @param columnNumber int
	 * @param resultSet ResultSet
	 * @return the string that is used in the <code>insert</code> statement. 
	 * Note, it needs to include quotes etc.
	 * @throws SQLException
	 */
	String getInsertValue(int columnNumber, ResultSet resultSet) throws SQLException;

	/**
	 * Insert the value into the given PreparedStatement.
	 * E.g. call preparedStatement.setByte(i, resultset.getByte("myColumn");
	 * @param preparedStatement
	 * @param preparedStatementIndex parameterIndex
	 * @param resultSet
	 * @param resultSetIndex 
	 * @throws SQLException 
	 */
	void insertIntoPreparedStatement(PreparedStatement preparedStatement, int preparedStatementIndex, ResultSet resultSet, int resultSetIndex) throws SQLException;
}
