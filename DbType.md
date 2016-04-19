# Introduction #

When using the DbCreationUtil, you tell the util what the data base type of a column is and how to populate it by specifying DbType in the DbConfig.


# Details #

The DbType has two functions.

  1. It is used by the DbCreationUtil when a new table is created (_getSqlType_). The implementation of the DbType should return a String that can be used in a create-statement. It must be valid for the database you use.

  1. It is used by the DbCreationUtil to retrieve the value that must be inserted into a column. Depending on the _usePreparedStatement_ indicator in the [DbConfig](DbConfig.md) the util will call the _getInsertValue_ or _insertIntoPreparedStatement_ method.  The value returned by the _getInsertValue_ method must be valid to be used in the insert-statement. Eg. must be surrounded by " for chars or must contain the function "to\_date(...)".

The implementations supplied in with this util are based on the [Apache Derby Data Types](http://db.apache.org/derby/docs/10.4/ref/crefsqlj31068.html#crefsqlj31068).