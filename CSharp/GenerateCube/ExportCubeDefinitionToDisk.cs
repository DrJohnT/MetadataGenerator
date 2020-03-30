using System.IO;
using System.Text;

namespace GenerateCube
{
    class ExportCubeDefinitionToDisk
    {
        /// <summary>
        /// Extracts the deployed cube structure from the deployed instance and outputs it to a folder
        /// </summary>
        /// <param name="db"></param>
        public static void ExportCubeDefinition(Microsoft.AnalysisServices.Tabular.Database db)
        {
            string outputDir = Path.Combine(Properties.Settings.Default.OutputDir, Properties.Settings.Default.AsDatabaseId);
            CreateOrCleanDirectory(outputDir);

            var dbJson = Microsoft.AnalysisServices.Tabular.JsonScripter.ScriptCreateOrReplace(db);
            string filePath = Path.Combine(outputDir, "CreateOrReplace.json");
            File.WriteAllText(filePath, dbJson);
            
            int counter = 0;
            int totalLines;

            string line;
            {
                System.IO.StreamReader file = new System.IO.StreamReader(filePath);
                while ((line = file.ReadLine()) != null)
                {
                    counter++;
                }
                totalLines = counter;
                file.Close();
            }



            StringBuilder newDb = new StringBuilder();
            counter = 0;
            {
                System.IO.StreamReader file = new System.IO.StreamReader(filePath);
                while ((line = file.ReadLine()) != null)
                {
                    if (counter == 0)
                    {
                        newDb.AppendLine(line);
                    } 
                    else 
                    {
                        if (counter > 5 && counter < totalLines - 2)
                        {
                            // strip off first few white space characters so the file formats match
                            newDb.AppendLine(line.Substring(4));
                        }
                    }
                    counter++;
                }
                file.Close();
            }

            
           

            filePath = Path.Combine(outputDir, "Model.bim");

            File.WriteAllText(filePath, newDb.ToString());
        }



        public static void CreateOrCleanDirectory(string outputDir)
        {
            DirectoryInfo directory = new DirectoryInfo(outputDir);
            if (!directory.Exists)
            {
                directory.Create();
            }

            // clean the output folder so we don't have old views hanging around
            foreach (FileInfo file in directory.GetFiles())
            {
                file.Delete();
            }
        }


    }
}
