# Introduction #

The DbCreationUtil is used to create and populate a database table with the result of the CSVParser. There are two ways to use the DbCreationUtil. First, get the ResultSet from the CSVParser and call the DbCreationUtil yourself. Second, tell the CSVParser to call the DbCreationUtil for you.

# Details #

Here is how you call the CSVParser and than the DbCreationUtil. Actually, you can call the DbCreationUtil with any ResultSet.

```
CSVParser csvParser = new CSVParser();
ResultSet rs = csvParser.parse(new BufferedReader(new FileReader("MyFile.csv")));
DbConfig config = new DbConfig();
// set any property you want on the DbConfig
DbCreationUtil dbc = new DbCreationUtil(config);
String jdbcUrl = dbc.createDB(rs);
```

Here is how you tell the CSVParser to call the DbCreationUtil for you.

```
CSVConfig csvConfig = new CSVConfig();
DbConfig dbconfig = new DbConfig();
// set any property you want on the DbConfig
CSVParser csvParser = new CSVParser(csvConfig, dbConfig);
ResultSet rs = csv.parseToDb(new BufferedReader(new FileReader("MyFile.csv")));
```

In both cases, you tell the DbCreationUtil how to create and populate your database table by supplying it a [DbConfig](DbConfig.md) object.