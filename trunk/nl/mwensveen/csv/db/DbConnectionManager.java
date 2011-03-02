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
import java.sql.SQLException;

/**
 * An implementation of this interface is used by the DbCreationUtil to manage the connection with the database. The manager is responsible for things
 * like creating and closing the connection and Transaction managerment.
 * 
 * @author Micha Wensveen
 */
public interface DbConnectionManager {

	/**
	 * This method is called by the DbCreationUtil to make the DbConfig available to this manager.
	 * 
	 * @param config the config to set
	 */
	public void setConfig(DbConfig config);

	/**
	 * @return the connection to use in the DbCreationUtil
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException;

	/**
	 * Called when the connection can be closed. If the connection has no autoCommit the implementation must perform a commit before closing the
	 * connection. It is up the the implementation to decide if the connection is actually closed, or remains open for further use (e.g. more Tables
	 * must be populated in the same transaction).
	 * 
	 * @throws SQLException
	 */
	public void close() throws SQLException;

}
