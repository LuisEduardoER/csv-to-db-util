# Introduction #

The CSVParser is the utility that you use to parses a CSV-file to a ResultSet. Besides that, you can tell the CSVParser to copy the data into a database (see [DBCreationUtil](DBCreationUtil.md)).


# Details #

The simplest use of the CSVParser is this.

```
    CSVParser csvParser = new CSVParser();
    ResultSet resultSet = csvParser.parse(new FileReader("MyFile.csv"));
```

That's it.

This will use a default configuration for parsing the file. But you can also tell the CSVParser to override the default configuration and specify your own.

```
    CSVConfig csvConfig = new CSVConfig();
    // here you override properties in the csvConfig();
    CSVParser csvParser = new CSVParser(csvConfig);
    ResultSet resultSet = csvParser.parse(new FileReader("MyFile.csv"));
```

The [CSVConfig](CSVConfig.md) object holds the configuration used by the parser.

Besides parsing the data into a ResultSet (as done bythe examples above), the CSVParser can copy your data to a database.

```
    CSVConfig csvConfig = new CSVConfig();
    // here you override properties in the csvConfig();
    DbConfig dbConfig = new DbConfig();
    // here you override properties in the dbConfig
    CSVParser csvParser = new CSVParser(csvConfig, dbConfig);
    ResultSet resultSet = csvParser.parseToDb(new FileReader("MyFile.csv"));
```

With the [DbConfig](DbConfig.md) you can tell the tool how to create the table and how to insert the data.