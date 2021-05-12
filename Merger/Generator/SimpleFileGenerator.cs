using Merger.Generator;
using System;
using System.Diagnostics;
using System.IO;

namespace Merger
{
    public class SimpleFileGenerator : IFileGenerator<FileEntry>
    {
        private string pathToSave;
        private Int64 desiredSize;
        private IEntryGenerator<FileEntry> entryGenerator;

        public string Path => pathToSave;

        public SimpleFileGenerator(IEntryGenerator<FileEntry> entryGenerator,
                                   string pathToSave,
                                   Int64 desiredSize)
        {
            this.pathToSave = pathToSave;
            this.desiredSize = desiredSize;
            this.entryGenerator = entryGenerator;
        }

        public void Generate()
        {
            Int64 writtenBytes = 0;
            if (File.Exists(pathToSave))
            {
                File.Delete(pathToSave);
            }

            using (StreamWriter writer = new StreamWriter(pathToSave))
            {
                do
                {
                    var entryTxt = entryGenerator.GetRandomEntry().ToString();
                    writer.WriteLine(entryTxt);
                    writtenBytes += entryTxt.Length + 1;
                }
                while (writtenBytes < desiredSize);
            }
        }
    }
}
