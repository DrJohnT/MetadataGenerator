using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;

namespace Generator
{
    public static class TemplateCommon
    {
        private const string TemplateFolder = "Common";

        public static void StandardMergeSP(DatabaseObject table, DirectoryInfo outputFolder, string LoadingSchema, string prefix)
        {
            string sqlScript = Templates.GetTemplateContent(TemplateFolder, "Load_StandardMerge.sql");
            IEnumerable<DatabaseColumn> joinColumns = table.Columns.Where(column => column.IsPrimaryKey == true);

            if (prefix == null)
                prefix = string.Empty;

            if (joinColumns.Count() == 0)
            {
                Console.WriteLine();
                Console.WriteLine("Target table has no primary key(s) so cannot create correctly formed MERGE statement {0}", table.DatabaseObjectName);
                Console.WriteLine();
            }            

            IEnumerable<DatabaseColumn> updateColumns;
            if (joinColumns.Count() >= 1)
                updateColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.nonUpdateableStandardColumns.Contains(column.DatabaseColumnName.ToLower()) && column.DatabaseColumnName != joinColumns.First().DatabaseColumnName);
            else
                updateColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.nonUpdateableStandardColumns.Contains(column.DatabaseColumnName.ToLower()));

            IEnumerable<DatabaseColumn> whenMatchedColumns;
            if (joinColumns.Count() >= 1)
                whenMatchedColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.allStandardColumns.Contains(column.DatabaseColumnName.ToLower()) && column.DatabaseColumnName != joinColumns.First().DatabaseColumnName);
            else
                whenMatchedColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.allStandardColumns.Contains(column.DatabaseColumnName.ToLower()));

            IEnumerable<DatabaseColumn> insertColumns = table.Columns.Where(column => column.DatabaseColumnName.ToLower() != DatabaseObject.updatedloadlogid && column.DatabaseColumnName.ToLower() != DatabaseObject.loadlogid && column.IsIdentity == false );

            IEnumerable<DatabaseColumn> identityColumns = table.Columns.Where(column => column.IsIdentity);
            string andClauseForDelete = string.Empty;
            if (table.DatabaseObjectName.ToLower().StartsWith("dim") && identityColumns.Count() >= 1)
            {
                andClauseForDelete = string.Format("and ({0} > -1) ", identityColumns.First().DatabaseColumnName);
            }

            string sql = string.Format(sqlScript,
                LoadingSchema,                      // {0} = loading schema name
                table.DatabaseObjectName,           // {1} = table name
                table.SchemaName,                   // {2} = table schema
                table.GetJoinColumnSql(joinColumns, "T", "S", false, false), // {3} = join criteia
                table.GetJoinColumnSql(whenMatchedColumns, "T", "S", false, true), // {4} = when matched critera
                table.GetUpdateColumnSql(updateColumns, "T", "S"),    // {5} = update column list
                table.GetColumnListSql(insertColumns, string.Empty),    // {6} = insert list
                table.GetColumnListSql(insertColumns, "S") ,             // {7} = insert value list
                prefix, // {8} = prefix for the stored proc name
                andClauseForDelete // {9} = and clause for the delete part of merge
            );

            string sqlPath = Path.Combine(outputFolder.FullName, string.Format("Load_{0}{1}.sql", prefix, table.DatabaseObjectName));
            Console.Write(".");
            System.IO.File.WriteAllText(sqlPath, sql);
        }

        public static void SimpleMergeSP(DatabaseObject table, DirectoryInfo outputFolder, string LoadingSchema, string prefix)
        {
            string sqlScript = Templates.GetTemplateContent(TemplateFolder, "Load_SimpleMerge.sql");
            IEnumerable<DatabaseColumn> joinColumns = table.Columns.Where(column => column.IsPrimaryKey == true);

            if (prefix == null)
                prefix = string.Empty;

            if (joinColumns.Count() == 0)
            {
                Console.WriteLine();
                Console.WriteLine("Target table has no primary key(s) so cannot create correctly formed MERGE statement {0}", table.DatabaseObjectName);
                Console.WriteLine();
            }

            IEnumerable<DatabaseColumn> updateColumns;
            if (joinColumns.Count() >= 1)
                updateColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.nonUpdateableStandardColumns.Contains(column.DatabaseColumnName.ToLower()) && column.DatabaseColumnName != joinColumns.First().DatabaseColumnName);
            else
                updateColumns = table.Columns.Where(column => column.IsPrimaryKey == false && column.IsIdentity == false && !DatabaseObject.nonUpdateableStandardColumns.Contains(column.DatabaseColumnName.ToLower()));

            IEnumerable<DatabaseColumn> insertColumns = table.Columns.Where(column => column.DatabaseColumnName.ToLower() != DatabaseObject.updatedloadlogid && column.DatabaseColumnName.ToLower() != DatabaseObject.loadlogid && column.IsIdentity == false);

            IEnumerable<DatabaseColumn> identityColumns = table.Columns.Where(column => column.IsIdentity);
            string andClauseForDelete = string.Empty;
            if (table.DatabaseObjectName.ToLower().StartsWith("dim") && identityColumns.Count() >= 1)
            {
                andClauseForDelete = string.Format("and ({0} > -1) ", identityColumns.First().DatabaseColumnName);
            }

            string sql = string.Format(sqlScript,
                LoadingSchema,                      // {0} = loading schema name
                table.DatabaseObjectName,           // {1} = table name
                table.SchemaName,                   // {2} = table schema
                table.GetJoinColumnSql(joinColumns, "T", "S", false, false), // {3} = join criteia
                table.GetUpdateColumnSql(updateColumns, "T", "S"),    // {4} = update column list
                table.GetColumnListSql(insertColumns, string.Empty),    // {5} = insert list
                table.GetColumnListSql(insertColumns, "S"),             // {6} = insert value list
                prefix, // {7} = prefix for the stored proc name
                andClauseForDelete // {8} = and clause for the delete part of merge
            );

            string sqlPath = Path.Combine(outputFolder.FullName, string.Format("Load_{0}{1}.sql", prefix, table.DatabaseObjectName));
            Console.Write(".");
            System.IO.File.WriteAllText(sqlPath, sql);
        }


        public static void StandardExceptSP(DatabaseObject table, DirectoryInfo outputFolder, string LoadingSchema, string prefix)
        {
            string sqlScript = Templates.GetTemplateContent(TemplateFolder, "Load_StandardExcept.sql");

            if (prefix == null)
                prefix = string.Empty;

            string sql = string.Format(sqlScript,
                LoadingSchema,                      // {0} = loading schema name
                table.DatabaseObjectName,           // {1} = table name
                table.SchemaName,                   // {2} = table schema
                table.GetColumnListSql(table.Columns.Where(column => !DatabaseObject.standardColumns.Contains(column.DatabaseColumnName.ToLower())), null),           // {3} = column list
                prefix // {4} = prefix for the stored proc name
                );

            string sqlPath = Path.Combine(outputFolder.FullName, string.Format("Load_{0}{1}.sql", prefix, table.DatabaseObjectName));
            Console.Write(".");
            System.IO.File.WriteAllText(sqlPath, sql);
        }

        public static void StandardInsertSP(DatabaseObject table, DirectoryInfo outputFolder, string LoadingSchema, string prefix)
        {
            string sqlScript = Templates.GetTemplateContent(TemplateFolder, "Load_StandardInsert.sql");

            if (prefix == null)
                prefix = string.Empty;

            string sql = string.Format(sqlScript,
                LoadingSchema,                      // {0} = loading schema name
                table.DatabaseObjectName,           // {1} = table name
                table.SchemaName,                   // {2} = table schema
                table.GetColumnListSql(table.Columns.Where(column => !DatabaseObject.standardColumns.Contains(column.DatabaseColumnName.ToLower())), string.Empty),          // {3} = column list
                prefix // {4} = prefix for the stored proc name
                );

            string sqlPath = Path.Combine(outputFolder.FullName, string.Format("Load_{0}{1}.sql", prefix, table.DatabaseObjectName));
            Console.Write(".");
            System.IO.File.WriteAllText(sqlPath, sql);
        }
    }
}
