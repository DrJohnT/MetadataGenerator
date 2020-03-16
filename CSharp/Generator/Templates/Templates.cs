using System;
using System.Collections.Generic;
using System.IO;


namespace Generator
{
    public static class Templates
    {
        private const string TemplatesFolder = "Templates";
        private const string CommonFolder = "Common";

        public static DirectoryInfo ParentTemplateDirectory()
        {
            return Utilities.GetDirectory(Path.Combine(Utilities.GetProjectDir(), TemplatesFolder));
        }

        public static DirectoryInfo GetTemplatesDir(string templateFolderName)
        {
            return Utilities.GetDirectory(Path.Combine(ParentTemplateDirectory().FullName, templateFolderName));
        }

        public static string GetTemplateContent(string TemplateFolder, string Template)
        {
            string sqlPath = Path.Combine(Templates.GetTemplatesDir(TemplateFolder).FullName, Template);
            if (File.Exists(sqlPath))
                return System.IO.File.ReadAllText(sqlPath);
            else
            {
                Console.WriteLine("Failed to find template {0} in {1}", Template, sqlPath);
                return string.Empty;
            }
        }

    }
}
