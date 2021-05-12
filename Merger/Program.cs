using Merger.Generator;
using System;
using System.Diagnostics;
using System.IO;

namespace Merger
{
    class Program
    {
        private const Int64 _1MB = 1024 * 1024;
        private const Int64 _1Gb = _1MB * 1024;

        private static string WorkDir = Directory.GetCurrentDirectory();

        static void Main(string[] args)
        {
            var bigFilePath = Path.Combine(WorkDir, "data.txt");
            var sortDir = Path.Combine(WorkDir, "Sort");

            if (!Directory.Exists(sortDir))
            {
                Directory.CreateDirectory(sortDir);
            }

            var lines = File.ReadAllLines("Chunks.txt");

            Process.Start("explorer", sortDir);

            IEntryGenerator<FileEntry> entryGenerator = new EntryGenerator(lines, 0, 100);

            IFileGenerator<FileEntry> gen = new SimpleFileGenerator(entryGenerator,
                bigFilePath,
                500 * _1MB);

            TimerHelper.WithTimer(() => gen.Generate(), "Generate");

            Sorter s = new Sorter(bigFilePath, 70000, sortDir);

            s.StartBlockSort();
        }        
    }
}
