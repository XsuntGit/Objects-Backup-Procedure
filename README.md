## Objects Backup Procedure
---
### Objective
The service stored procedure [**`master.dbo.Sys_Backup_Object.sql`**](https://github.com/XsuntGit/Objects-Backup-Procedure/blob/master/dbo.Sys_Backup_Object.sql) has been created to replace current approach which is being used to create a local copy of a table before any change gets released in case previous data needs to be kept and be accessed at any time.

### Main workflow description

The main concept of new method is to create a backup of an object (a table) which will be stored outside of the database. All output files will be added into a single `.zip` file. Here is the description of all output files that wiull be included:

Num | Extention | Description
---|---|---
1 | `<TableName>.sql` | `t-sql` file which will contain object definition based on `CREATE TABLE...` statement including identity, PK info, indexes, triggers, default constraits, etc.
2 | `<TableName>.txt` | All the table's data will be extracted into `.txt` flat file. This process is based on [bcp utility](https://docs.microsoft.com/en-us/sql/tools/bcp-utility).
3 | `<TableName>.xml` | Additional `.xml` [format file](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/xml-format-files-sql-server) will be created which can be used for further necessary [BULK INSERT](https://docs.microsoft.com/en-us/sql/relational-databases/import-export/use-a-format-file-to-bulk-import-data-sql-server) process in case the data needs to be loaded into a the same table or a any table with the same layout.

The output `.zip` will be compressed and pasword-protected. Once the process is done, an email will be issued to a process owner which will include:
- Full object name which has been backed up;
- Full path to the newly created `.zip` file;
- 15 character randomly generated password;
> Please note, that the password is generated randomly. The password will be known to only a person who runs the process and an email will be sent to only that person.

The stored procedure has 4 mandatory parameters. All the parameters needs to be specified according to their ordonal position. Below is the description of all the parameters:

Num | Parameter | Type | Description
---|---|---|---
1 | `@DatabaseName` | `SYSNAME` | The database name where the object is located. Usually it's a name of a user's database. For instance, `Enbrel_Production`, `Enbrel_Staging`, etc.
2 | `@SchemaName` | `SYSNAME` | The schema name which owns a certain object. Usually it's `dbo`, but it can be a different schema name.
3 | `@TableName` | `SYSNAME` | The object (Table) name. An exact name needs to be specified, otherwise an object won't be found.
4 | `@FilePath` | `NVARCHAR(256)` | Destination path where a `.zip` file will be created and stored. Recommended to use **`\\172.28.99.15\amg-obj-bck\`**

Additional scripts have been used in the stored procedures which can be referenced by the following link: <https://github.com/XsuntGit/Objects-Backup-Procedure/tree/master/Scripts>

### Examples

1. Extract `dbo.Ashfield_To_XSUNT` table from `Enbrel_Staging` database into `\\172.28.99.15\amg-obj-bck\` shared folder:
```sql
EXEC master.dbo.Sys_Backup_Object 'Enbrel_Staging', 'dbo', 'Ashfield_To_XSUNT', '\\172.28.99.15\amg-obj-bck\'
```
2. Extract `dbo.inCRMClaims` table from `EnbrelReporting_Production` database into `\\172.28.99.15\amg-obj-bck\` shared folder:

```sql
EXEC master.dbo.Sys_Backup_Object 'EnbrelReporting_Production', 'dbo', 'inCRMClaims', '\\172.28.99.15\amg-obj-bck\'
```
