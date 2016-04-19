# Introduction #

The DbCreationUtil uses an implentation of the DbConnectionManager to get a connection to the database.


# Details #

When the DbCreationUtil needs a connection to the database, it uses a DbConnectionManager implentation. The default one is DefaultDbConnectionManager. You can write your own implementation and use it by setting the dbConnectionManager on the DbConfig object.

You should create an implementation yourself if you want to:
  * use commitment controle and your database does not use autocommit.
  * you want to parese more than one CSV file in one transaction.
  * you want to use connection pooling or jndi-lookup.