# Bulk CSV Data Loading in MySQL
MySQL provides the `LOAD DATA INFILE `/ `LOAD DATA LOCAL INFILE` statement for high-performance bulk data loading. This method directly reads data files from the server or client filesystem and inserts them into database tables.

## Limitation in This Environment
In this project, the MySQL server is running with restrictions that prevent the use of LOAD DATA INFILE, such as:
- `local_infile` is disabled on the MySQL server
- File system access is restricted (common in managed or shared environments)
- Security policies disallow direct file loading from the database process

Because of these limitations, `LOAD DATA INFILE` cannot be executed from SQL scripts or stored procedures.

## Alternative Approach: Python-Based Bulk Loading
To work around this restriction, a Python-based data loading process is used. 
The Python script reads CSV files and inserts data into MySQL using a database connector (`mysql-connector-python` or `PyMySQL`).

##### This approach:
- Avoids MySQL server file access restrictions
- Works in hosted and restricted environments
- Allows additional validation and transformation before insertion

#### Example: MySQL `LOAD DATA INFILE` (Reference)
Note: This example is for reference only and will work only if server permissions allow it.
```sql
LOAD DATA LOCAL INFILE '/path/to/data.csv'
INTO TABLE employee
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
```
### Enabling `LOAD DATA INFILE`
If server access is available, `LOAD DATA INFILE` can be enabled as follows:

```sql
 SHOW GLOBAL VARIABLES LIKE 'local_infile';
```
```sql
 SET GLOBAL local_infile = 1;
```
Client connection must also allow it:
```sql
mysql --local-infile=1 -u user -p
```
### Summary
Due to MySQL server security and permission restrictions, 
bulk CSV loading is implemented using a Python-based solution instead of `LOAD DATA INFILE`. 
This ensures compatibility across restricted environments while maintaining reliable and controlled data ingestion.
