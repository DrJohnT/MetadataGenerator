using System.ComponentModel;
using System.Xml.Serialization;
using System.Linq;
using System.Collections.Generic;
using System;

namespace Generator
{
    public class DatabaseColumn : IDatabaseColumn
    {
        [XmlAttribute]
        public int DatabaseInfoId { get; set; }

        [XmlAttribute]
        public string DatabaseName { get; set; }

        [XmlAttribute]
        public string SchemaName { get; set; }

        [XmlAttribute(AttributeName = "ObjectId")]
        public int DatabaseObjectId { get; set; }

        [XmlAttribute(AttributeName = "ObjectName")]
        public string DatabaseObjectName { get; set; }

        [XmlAttribute(AttributeName = "ColumnId")]
        public int DatabaseColumnId { get; set; }

        [XmlAttribute(AttributeName = "ColumnName")]
        public string DatabaseColumnName { get; set; }

        private string databaseColumnPrefix = "A";
        public string DatabaseColumnPrefix {
            get { return this.databaseColumnPrefix; }
            set { this.databaseColumnPrefix = value; }
        }

        public override string ToString ()
        {
            return DatabaseColumnName;
        }


        private string databaseColumnSortOrder = null;
        public string DatabaseColumnSortOrder
        {
            get
            {
                if (this.databaseColumnSortOrder == null)
                    return DatabaseColumnName;
                else
                    return databaseColumnSortOrder;
            }
            set { this.databaseColumnSortOrder = value; }
        }

        [XmlAttribute]
        public string DataType { get; set; }

        [XmlAttribute]
        public int Length { get; set; }

        [XmlAttribute]
        public int Precision { get; set; }

        [XmlAttribute]
        public int Scale { get; set; }

        [XmlAttribute()]
        [DefaultValueAttribute(true)]
        public bool IsNullable { get; set; }

        [XmlAttribute]
        [DefaultValueAttribute(false)]
        public bool IsIdentity { get; set; }

        public bool IsPrimaryKey { get; set; }

        [XmlAttribute]
        public int? MaxLengthInTable { get; set; }

        [XmlAttribute]
        public int? MaxValueInTable { get; set; }

        [XmlAttribute]
        public int? NumberOfNulls { get; set; }

        [XmlAttribute]
        public int? NumberOfBlanks { get; set; }

        private string targetColumnName = null;
        public string TargetColumnName {
            get {
                if (this.targetColumnName == null)
                    return DatabaseColumnName;
                else
                    return targetColumnName;
            }
            set { this.targetColumnName = value; }
        }

        private string targetDataType = null;
        public string TargetDataType
        {
            get
            {
                if (this.targetDataType == null)
                    return DataType;
                else
                    return targetDataType;
            }
            set { this.targetDataType = value; }
        }

        private int targetLength = 0;
        public int TargetLength {
            get
            {
                if (this.targetLength == 0)
                    return Length;
                else
                    return targetLength;
            }
            set { this.targetLength = value; }
        }

        private int targetPrecision = 0;
        public int TargetPrecision {
            get
            {
                if (this.targetPrecision == 0)
                    return Precision;
                else
                    return targetPrecision;
            }
            set { this.targetPrecision = value; }
        }

        private int targetScale = 0;
        public int TargetScale {
            get
            {
                if (this.targetScale == 0)
                    return Scale;
                else
                    return targetScale;
            }
            set { this.targetScale = value; }
        }

        public bool TrimWhitespace { get; set; }

        public bool SpecificLookupsDone { get; set; }

        public bool UseColumn { get; set; }

        public string GetUnknownValueForView(string unknownStringValue, string unknownShortStringValue, int unknownInt, string unknownDateTime, bool useNullIfPossible)
        {
            string unknownValue;
            if (IsNullable && useNullIfPossible)
            {
                unknownValue = "null";
            }
            else
            {

                switch (DataType.ToLower())
                {
                    case "nvarchar":
                    case "nchar":
                    case "varchar":
                    case "char":
                        // Long columns use full UNKNOWN, short columns use just U
                        // nvarchar(max) returns a length of -1, so treat as a long column
                        if (Length == -1 || Length >= 7)
                            unknownValue = unknownStringValue;
                        else
                            unknownValue = unknownShortStringValue;

                        if (DatabaseColumnName.Contains("Currency") && Length >= 3)
                        {
                            unknownValue = "N'UNK'";
                        }
                        break;

                    case "date":
                    case "datetime":
                    case "datetime2":
                    case "datetimeoffset":
                    case "smalldatetime":
                        unknownValue = "'" + unknownDateTime + "'";
                        break;

                    case "bigint":
                    case "int":
                    case "decimal":
                    case "numeric":
                    case "money":
                    case "smallmoney":
                        unknownValue = unknownInt.ToString();
                        break;

                    case "tinyint":
                    case "bit":
                        unknownValue = "0";
                        break;

                    default:
                        // Use column as it is, without setting a default
                        unknownValue = "null";
                        break;
                }
            }
            return unknownValue;
        }

        public string GetDataTypeModifiers(bool makeAllNullable, bool useTarget)
        {
            string modifiers = string.Empty;

            string dataType = DataType.ToLower();
            if (useTarget)
                dataType = TargetDataType;

            switch (dataType)
            {
                case "nvarchar":
                case "nchar":
                case "varchar":
                case "char":
                case "varbinary":
                    modifiers += "(";

                    if (Length == -1)
                        modifiers += "max";
                    else
                    {
                        if (useTarget)
                            modifiers += TargetLength.ToString();
                        else
                            modifiers += Length.ToString();
                    }
                    modifiers += ")";
                    break;

                case "decimal":
                case "numeric":
                    modifiers += "(";
                    modifiers += Precision.ToString();
                    modifiers += ",";
                    modifiers += Scale.ToString();
                    modifiers += ")";
                    break;

                    //case "date":
                    //case "datetime":
                    //case "datetime2":
                    //case "datetimeoffset":
                    //case "smalldatetime":
                    // "bigint", "int", "tinyint", "bit"

            }
            if (!IsNullable && !makeAllNullable)
                modifiers += " not null";
            else
                modifiers += " null";
            return modifiers;
        }

    }

    // Custom comparer for the Product class
    public class ColumnComparer : IEqualityComparer<DatabaseColumn>
    {
        public bool Equals(DatabaseColumn x, DatabaseColumn y)
        {
            // Check whether the compared objects reference the same data.
            if (Object.ReferenceEquals(x, y)) return true;

            return x.DatabaseColumnName == y.DatabaseColumnName;
        }

        public int GetHashCode(DatabaseColumn column)
        {
            // Check whether the object is null
            if (Object.ReferenceEquals(column, null)) return 0;
            //Calculate the hash code for the column.
            return column.DatabaseColumnName.GetHashCode();
        }
    }
}
