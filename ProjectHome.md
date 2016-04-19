There are many csv-parsers out there. So why another one?
Because none of the others did what I needed.

So what is special about this csv-to-db utility?
I'll tell you.

### CSV Parser Features ###
  1. Exports from outlook-addressbook can have carriage-returns in the address (if you entered it that way in outlook). This utility will handle that correctly.
  1. It converts a csv-file into a java.sql.ResultSet so you can use it the same as you use a 'select' sql statement
  1. Because you might want to make selections on the file, you can put it in a DataBase.
  1. Some CSV files have column-names on the first row. These will be used as the metadata for the ResultSet.
  1. Internationalization. Different date-formats and decimal points.
  1. Not all CSV files use a comma to seperate the fields, sometime tabs are used. This utility can.

When you want to put the file into a DataBase, you can configure what the DataBase table will look like.

### DB Creation Features ###
  1. You can supply a jdbc connection url to use, or use the default Derby.
  1. You can use an existing table, or create a new one.
  1. You can tell what data-type the fields of the table will be.
  1. Many data-types are already there, but you can make your own implementation of the DbType so this utility can handle your database.
  1. You can add an extra column enabling you to create a primary key.
  1. Some CSV files have column-names on the first row. These will be used as the column-names for the table.
  1. Some CSV files don't have column-names on the first row. You can supply the column-names yourself.
  1. Some CSV file do have column-names on the first row, but you don't want to use those but supply your own. Also possible.