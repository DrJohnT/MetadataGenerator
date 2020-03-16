using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace Generator
{
    public static class TemplateComponents
    {
        private const string TemplateFolder = "Components";

        public static string AutogenWarning()
        {
            return Templates.GetTemplateContent(TemplateFolder, "AutogenWarning.sql");
        }

        public static string PleaseAdjust()
        {
            return Templates.GetTemplateContent(TemplateFolder, "PleaseAdjust.sql");
        }

        public static string CheckInsertView(string LoadingSchema, string DatabaseObjectName, string AllColumns)
        {
            string sqlScript = Templates.GetTemplateContent(TemplateFolder, "CheckInsertView.sql");
            return string.Format(sqlScript, LoadingSchema, DatabaseObjectName, AllColumns);
        }
    }
}
