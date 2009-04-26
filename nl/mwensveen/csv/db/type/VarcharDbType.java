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
package nl.mwensveen.csv.db.type;

import java.sql.ResultSet;
import java.sql.SQLException;

import nl.mwensveen.csv.db.type.api.DbType;

/**
 * @author mwensveen
 *
 */
public class VarcharDbType implements DbType {
	private int length = 32672; 

	
	public VarcharDbType() {
		super();
	}

	public VarcharDbType(int length) {
		super();
		this.length = length;
	}

	/**
	 * @throws SQLException 
	 * @see nl.mwensveen.csv.db.type.api.DbType#getInsertValue(int, java.sql.ResultSet)
	 */
	public String getInsertValue(int columnNumber, ResultSet resultSet) throws SQLException {
		String s = resultSet.getString(columnNumber);
		return "'" + s + "'";
	}

	/**
	 * @see nl.mwensveen.csv.db.type.api.DbType#getSqlType()
	 */
	public String getSqlType() {
		return "VARCHAR(" + length + ")";
	}

	/**
	 * @param length the length to set
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * @return the length
	 */
	public int getLength() {
		return length;
	}

}
