using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;

namespace Generator
{
    public class SchemaInfo : ISchemaInfo
    {
        [XmlAttribute]
        public int DatabaseInfoId { get; set; }

        [XmlAttribute]
        public string SchemaName { get; set; }

        [XmlAttribute]
        public string StagingAreaSchema { get; set; }
    }
}
