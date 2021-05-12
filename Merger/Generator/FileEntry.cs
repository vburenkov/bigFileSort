using System;

namespace Merger
{
    public class FileEntry : IComparable
    {
        public int Num { get; set; }
        public string Data { get; set; }
        public int CompareTo(object obj)
        {
            if (obj == null)
            {
                return 1;
            }

            FileEntry other = obj as FileEntry;
            if (other == null)
            {
                throw new ArgumentException($"Object is not of type {nameof(FileEntry)}");
            }

            var dataComparison = this.Data.CompareTo(other.Data);
            if (dataComparison != 0)
            {
                return dataComparison;
            }

            return this.Num.CompareTo(other.Num);
        }

        public override string ToString()
        {
            return $"{Num}. {Data}";
        }

        public static FileEntry Parse(string s)
        {
            if (string.IsNullOrEmpty(s))
            {
                throw new FormatException("Can not parse. String is empty");
            }

            var dotIndex = s.IndexOf('.');

            if (dotIndex <= 0)
            {
                throw new FormatException($"Can not parse {s}");
            }

            var numStr = s.Substring(0, dotIndex);
            var num = int.Parse(numStr);
            var dataStr = s.Substring(dotIndex+1, s.Length-dotIndex-1);
            var data = dataStr.Trim();

            return new FileEntry()
            {
                Num = num,
                Data = data
            };           
            
        }
    }
}
