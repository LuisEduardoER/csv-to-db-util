# Introduction #

When you parse a csv-file to a resultset (or database) you can tell the CSVParser how to handle the parsing. This is done by using a CSVConfig object.


# Details #

Here is an overview of the properties you can set in the CSVConfig


**columnNames**
|type|Map<Integer, String> |
|:---|:--------------------|
|usage|With this property you can set the column names. Set the column number (starting with 1) as key in the map. <br>If the CSV file does not contain a first row with column names, the names you specify with this property will be used.<br>If the CSV file does contain column names, you can use this property to override the ones in the file.<br>
<tr><td>default</td><td>If a column name cannot be determined based on this property or on the csv-file, the CSVParser will create a column name: Field{columnnumer}</td></tr></tbody></table>

**datePattern**
|type|String|
|:---|:-----|
|usage|With this property you can set the datepatern to be used by the CSVParser when it converts a String into a Date.<br>The parsing will only be done when you call the <i>getDate</i> method on the ResultSet.This way you can set this property to the correct pattern before parsing different columns.<br>
<tr><td>default</td><td>"yyyyMMdd"</td></tr></tbody></table>

<b>decimalPoint</b>
<table><thead><th>type</th><th>char</th></thead><tbody>
<tr><td>usage</td><td>With this property you can set the decimalPoint that is used when the CSVParser converts a String into a number.</td></tr>
<tr><td>default</td><td>.   </td></tr></tbody></table>

<b>seperator</b>
<table><thead><th>type</th><th>char</th></thead><tbody>
<tr><td>usage</td><td>With this property you can tell the CSVParser which character is used in the CSV file to seperate the columns.</td></tr>
<tr><td>default</td><td>,   </td></tr></tbody></table>

<b>startWithMetaDataRow</b>
<table><thead><th>type</th><th>boolean</th></thead><tbody>
<tr><td>usage</td><td>With this property you tell the CSVParser that the CSV file begins with a row with metadata (=column names) (true) or if the first row already contains data (false).</td></tr>
<tr><td>default</td><td>true   </td></tr>