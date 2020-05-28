using System.Linq;
using System.Collections.Generic;
using System.Xml.Serialization;
using System.Text;
using System;

namespace Generator
{
    public class DatabaseObject : IDatabaseObject
    {
        public List<DatabaseColumn> Columns;

        private string targetObjectName = null;
        public const string updatedloadlogid = "updatedloadlogid";
        public const string loadlogid = "loadlogid";
        public const string databaseinfoid = "databaseinfoid";
        public static string[] standardColumns = { updatedloadlogid };
        public static string[] allStandardColumns = { loadlogid, updatedloadlogid };
        public static string[] nonUpdateableStandardColumns = { loadlogid };


        public DatabaseObject()
        {
            // initialize the list of columns in the Database Object
            this.Columns = new List<DatabaseColumn>();
        }

        [XmlAttribute]
        public int DatabaseInfoId { get; set; }

        [XmlAttribute]
        public int DatabaseGroupId { get; set; }

        [XmlAttribute]
        public string DatabaseName { get; set; }


        [XmlAttribute(AttributeName = "ObjectId")]
        public int DatabaseObjectId { get; set; }

        [XmlAttribute(AttributeName = "ObjectName")]
        public string DatabaseObjectName { get; set; }

        public string TargetObjectName
        {
            get
            {
                if (this.targetObjectName == null)
                    return DatabaseObjectName;
                else
                    return targetObjectName;
            }
            set { this.targetObjectName = value; }
        }

        [XmlAttribute]
        public string SchemaName { get; set; }

        [XmlAttribute]
        public string StagingAreaSchema { get; set; }

        [XmlAttribute]
        public int ColumnCount { get; set; }

        [XmlAttribute(AttributeName = "ObjectType")]
        public string DatabaseObjectType { get; set; }

        [XmlAttribute]
        public long NumberOfRows { get; set; }
        public bool UseObject { get; set; }
        public string LoadingPattern { get; set; }

        public string GetColumnListSql()
        {
            return GetColumnListSql(null, null);
        }

        public string GetColumnListSql(IEnumerable<DatabaseColumn> filteredColumns)
        {
            return GetColumnListSql(filteredColumns, null);
        }

        public string GetColumnListSql(IEnumerable<DatabaseColumn> filteredColumns, string prefixOverride)
        {
            StringBuilder columnList = new StringBuilder();

            if (filteredColumns == null)
                filteredColumns = Columns.Where(x => x.DatabaseColumnName.ToLower() != loadlogid);

            // [prefix].[column]
            string format = "\r\n\t\t\t{2}.{1}{0}";
            if (prefixOverride == string.Empty)
                format = "\r\n\t\t\t{1}{0}";

            // Indent by number tabs
            //string indentTabs = new string('\t', 3);
            string delimiter = ",";

            foreach (DatabaseColumn column in filteredColumns.OrderBy(x => x.DatabaseColumnSortOrder))
            {
                string prefix = column.DatabaseColumnPrefix;
                if (prefixOverride != null)
                    prefix = prefixOverride;

                if (column.DatabaseColumnName.ToLower() == updatedloadlogid && prefix == "S")
                    columnList.AppendFormat("\r\n{0}{1}", delimiter, "@LoadLogId");
                else
                {
                    columnList.AppendFormat(format, delimiter, column.DatabaseColumnName, prefix);
                }
            }
            string retValue = columnList.ToString();
            if (retValue.Length > 0)
                // remove trailing comma
                retValue = retValue.Substring(0, retValue.Length - 1);
            return retValue;
        }

        public string GetJoinColumnSql(string prefixA, string prefixB)
        {
            IEnumerable<DatabaseColumn> filteredColumns = Columns.Where(column => column.IsPrimaryKey == true);

            // Return formatted join SQL for the chosen columns
            return GetJoinColumnSql(filteredColumns, prefixA, prefixB, false, false);
        }

        public string GetJoinColumnSql(IEnumerable<DatabaseColumn> filteredColumns, string schemaA, string schemaB, bool includeOn, bool useNotEqual)
        {
            bool firstColumn = true;
            string joinColumnSql = string.Empty;

            if (!string.IsNullOrEmpty(schemaA))
                schemaA = schemaA + ".";

            if (!string.IsNullOrEmpty(schemaB))
                schemaB = schemaB + ".";

            // Get target schema length and add one for the full stop between it and the column name
            int schemaLength = schemaA.Length + 1;

            foreach (DatabaseColumn column in filteredColumns.OrderBy(x => x.DatabaseColumnName))
            {
                if (firstColumn)
                {
                    if (includeOn)
                        joinColumnSql += " on\t";
                    else
                        joinColumnSql += "\n\t\t\t\t";
                    firstColumn = false;
                }
                else
                {
                    if (useNotEqual)
                        joinColumnSql += "\n\t\t\tor\t";
                    else
                        joinColumnSql += "\n\t\t\tand\t";
                }
                if (useNotEqual)
                {
                    string isnullReplacement = "''"; // for strings
                    switch (column.DataType)
                    {
                        case "smallmoney":
                        case "money":
                        case "decimal":
                        case "numeric":
                        case "bigint":
                        case "int":
                        case "tinyint":
                        case "bit":
                            isnullReplacement = "0";
                            break;

                        case "date":
                        case "datetime":
                        case "datetime2":
                        case "datetimeoffset":
                        case "smalldatetime":
                            isnullReplacement = "'19000101'";
                            break;

                    }
                    joinColumnSql += string.Format("ISNULL({0}{1},{2}) <> ISNULL({3}{4},{2})", schemaA , column.DatabaseColumnName, isnullReplacement, schemaB, column.DatabaseColumnName);
                }
                else
                    joinColumnSql += schemaA + column.DatabaseColumnName + " = " + schemaB + column.DatabaseColumnName;
                
            }

            return joinColumnSql + "\n\t\t";
        }

        public string GetViewColumnListSql()
        {
            return GetViewColumnListSql(null);
        }

        public string GetViewColumnListSql(IEnumerable<DatabaseColumn> filteredColumns)
        {
            string columnList = string.Empty;

            // Should pass in the correctly filtered list of columns for public views in filteredColumns
            // If filteredColumns == null then remove standard columns
            if (filteredColumns == null)
                filteredColumns = Columns.Where(column => (!standardColumns.Contains(column.DatabaseColumnName.ToLower())));

            int iColCount = 1;

            // run through the columns in alphabetical order
            foreach (DatabaseColumn column in filteredColumns.OrderBy(x => x.DatabaseColumnSortOrder))
            {
                if (iColCount > 1)
                    columnList += ",";

                string unknownValue = column.GetUnknownValueForView("N'UNKNOWN'", "N'U'", 0, "19000101", false);

                if (column.DataType.ToString() == "bit")
                {
                    columnList += string.Format("\n\tcase\n\t\twhen {0}.{1} = 1 then N'Yes'\n\t\twhen {0}.{1} = 0 then N'No'\n\t\telse N'Unknown'\n\tend\t\t\t\t\t\t\tas {1}", column.DatabaseColumnPrefix, column.DatabaseColumnName);
                }
                else if (!column.IsNullable || unknownValue == string.Empty)
                {
                    columnList += string.Format("\n\t{0}.{1}", column.DatabaseColumnPrefix, column.DatabaseColumnName);
                }
                else
                {
                    columnList += string.Format("\n\tisnull({0}.{1},{2})\t\t\tas {1}", column.DatabaseColumnPrefix, column.DatabaseColumnName, unknownValue);
                }

                iColCount++;
            }
            return columnList;
        }

        public string GetUpdateColumnSql(string targetSchema, string sourceSchema)
        {
            return GetUpdateColumnSql(null, targetSchema, sourceSchema);
        }

        public string GetUpdateColumnSql(IEnumerable<DatabaseColumn> filteredColumns, string targetSchema, string sourceSchema)
        { 
            if (filteredColumns == null)
                filteredColumns = Columns.Where(column => column.IsPrimaryKey == false && column.DatabaseColumnName.ToLower() != loadlogid);

            string updateColumnSql = "\n\t\t\t";

            // Get target schema length and add one for the full stop between it and the column name
            int schemaLength = targetSchema.Length + 1;
            
            foreach (DatabaseColumn column in filteredColumns.OrderBy(x => x.DatabaseColumnName))
            {
                updateColumnSql += "\n\t\t\t";

                if (column.DatabaseColumnName.ToLower() == loadlogid)
                {
                    updateColumnSql += targetSchema + "." + column.DatabaseColumnName + " = @LoadLogId,";
                }
                else
                {
                    updateColumnSql += targetSchema + "." + column.DatabaseColumnName + " = " + sourceSchema + "." + column.DatabaseColumnName + ",";
                }
                
            }
            // remove trailing comma
            return updateColumnSql.Substring(0, updateColumnSql.Length - 1);
        }



        internal string GetPkName()
        {
            return Columns.First(column => column.IsPrimaryKey == true).DatabaseColumnName;
        }


        public string GetUnknownList(IEnumerable<DatabaseColumn> filteredColumns, string unknownStringValue, string unknownShortStringValue, int unknownInt)
        {
            string columnList = string.Empty;

            int iColCount = 1;
            foreach (DatabaseColumn column in filteredColumns.OrderBy(x => x.DatabaseColumnSortOrder))
            {
                if (iColCount > 1)
                    columnList += ",";

                columnList += column.GetUnknownValueForView(unknownStringValue, unknownShortStringValue, unknownInt, "19000101", true);

                iColCount++;
            }
            return columnList;
        }
    }
}
