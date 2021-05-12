using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Merger
{
    public class EntryGenerator : IEntryGenerator<FileEntry>
    {
        private List<string> chunks;
        private int from;
        private int to;
        private Random random;

        public EntryGenerator()
        {
        }

        public EntryGenerator(IEnumerable<string> chunks, int from, int to)
        {
            this.chunks = chunks.ToList();
            this.@from = from;
            this.to = to;
            random = new Random();
        }

        public FileEntry GetRandomEntry()
        {
            return new FileEntry()
            {
                Data = GetRandomString(),
                Num = GetRandomNum()
            };
        }

        private int GetRandomNum()
        {
            return this.random.Next(this.@from, this.to);
        }

        private string GetRandomString()
        {
            return chunks[this.random.Next(0, chunks.Count)];
        }
    }
}
