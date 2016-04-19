# Introduction #

When the DbCreationUtil creates a table and populates it, it needs to have information about that table. You supply this info with the DbConfig object.


# Details #

Here is an overview of the properties you can set on the DbConfig

**jdbcUrl**
|type|String|
|:---|:-----|
|usage|With this property you specify the jdbc connection url that the utility must use. And after the DbCreationUtil is finished you can retrieve the jdbc connection url that was used (so you can get the default jdbcUrl if needed).|
|default|jdbc:derby:_databaseName_;create=true;|

**dataBaseName**
|type|String|
|:---|:-----|
|Usage|With this property you can pecify the database name that the DbCreationUtil uses when no jdbcUrl is given|
|default|CSVdb |

**tableName**
|type|String|
|:---|:-----|
|usage|With this property you tell the utility the table name that needs to be created and populated.|
|default|csvTable|

**createTable**
|type|boolean|
|:---|:------|
|usage|With this property you indicate whether the utility must first create a new table (true) or if an existing table is used (false)|
|default|true   |

**dataTypes**|type|Map<String, [DbType](DbType.md)>|
|:---|:-------------------------------|
|useage|With this map you specify what the type of a column in the table is. As key you specify the columnName (corresponding to the ResultSetMetaData) or the column number as Integer.toString(). Note that the first column in a ResultSet is column 1.|
|default|for any column that cannot be found in the dataTypes, the type LongVarcharDbType is used.|

**extraColumn**
|type|[DbType](DbType.md)|
|:---|:------------------|
|usage|With this property you can tell the DbCreationUtil to add an extra column to the table, that is not in the ResultSet. Typical use is adding a primarykey. If this property is null, no extra column is created.|
|default|null               |

**extraColumnName**
|type|String|
|:---|:-----|
|usage|With this property you specify the name the DbCreationUtil will you when creating an _extraColumn_.|
|default|null  |

**dbConnectionManager**
|type|[DbConnectionManager](DbConnectionManager.md)|
|:---|:--------------------------------------------|
|usage|With this property you can set the connection manager that must be used|
|default|DefaultDbConnectionManager                   |

**usePreparedStatement**
|type|boolean|
|:---|:------|
|usage|With this property you tell the DbCreationUtil that the insert statements must be done using a prepared statement (true) or must be done by creating a sql String (false)|
|default|false  |