## Why this project ##

The basic idea behind this project was the need to parse some CSV files and use them as input in a java program. I had a few requirements for a parser.
  * Use the CSV file the same as I could use a DB. I.e. parse the data to a ResultSet.
  * Some CSV files start with a Header row (MetaData), some don't start with that. A utility must be able to handle both.
  * Being Dutch, I don't want to use English Headers, I want to override them with some nice dutch titles.
  * The parsed data must be available in the correct type (e.g. Date), not only as String.
  * There must be a possiblility to set some localization for data-formats and decimal points.

After I searched for a tool that could meet these requirements, I decided that I needed to write my own tool. After I made this decision, I thought of some other usefull features.
  * Put the parsed data into a database. That way I can do actual queries on the data of the csv file.
  * As storing the data in a database would be temporary, the tools should be able to create a database Table with columns of the correct type.

To meet my requirements and wishes, I created to utilities in the project.
First there is the **[CSVParser utility](CSVParser.md)** and than there is the **[DbCreationUtil utility](DbCreationUtil.md)**.