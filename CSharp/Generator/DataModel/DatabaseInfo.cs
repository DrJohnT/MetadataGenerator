using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Generator
{
    public class DatabaseInfo : IDatabaseInfo
    {
        [XmlAttribute]
        public int DatabaseInfoId { get; set; }

        [XmlAttribute]
        public int DatabaseGroupId { get; set; }

        [XmlAttribute]
        public string DatabaseName { get; set; }

        [XmlAttribute]
        public string DatabaseDescription { get; set; }

        [XmlAttribute]
        public string ServerName { get; set; }

        public bool ImportMetadata { get; set; }

        [XmlAttribute]
        public string DatabaseGroup { get; set; }

        [XmlAttribute]
        public string pkPrefix { get; set; }

        public List<SchemaInfo> Schemas;
    }
}
