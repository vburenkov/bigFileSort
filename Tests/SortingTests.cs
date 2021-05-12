using NUnit.Framework;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace Merger.Tests
{
    [TestFixture]
    public class SortingTests
    {
        [Test]
        public void SortTestList()
        {
            var f1 = new FileEntry() { Num = 1, Data = "zzzzz" };
            var f2 = new FileEntry() { Num = 2, Data = "ggg" };
            var f3 = new FileEntry() { Num = 2, Data = "yyy" };
            var f4 = new FileEntry() { Num = 2, Data = "aav" };
            List<FileEntry> entries = new List<FileEntry>() { f1, f2, f3, f4 };

            var sorted = entries.OrderBy(e => e).ToList();

            Assert.Zero(sorted[0].CompareTo(f4));
            Assert.Zero(sorted[1].CompareTo(f2));
            Assert.Zero(sorted[2].CompareTo(f3));
            Assert.Zero(sorted[3].CompareTo(f1));
        }

        [Test]
        public void SortFileTest()
        {
            var f1 = new FileEntry() { Num = 1, Data = "zzzzz" };
            var f2 = new FileEntry() { Num = 2, Data = "ggg" };
            var f3 = new FileEntry() { Num = 2, Data = "yyy" };
            var f4 = new FileEntry() { Num = 2, Data = "aav" };
            List<FileEntry> entries = new List<FileEntry>() { f1, f2, f3, f4 };

            var tempFile = Path.GetTempFileName();
            var sortedTempFile = Path.GetTempFileName();
            Sorter.WriteList(tempFile, entries);
            Sorter.Sort(tempFile, sortedTempFile, false);
            var sorted = File.ReadAllLines(sortedTempFile).Select(l => FileEntry.Parse(l)).ToList();

            // clean
            File.Delete(tempFile);
            File.Delete(sortedTempFile);

            Assert.Zero(sorted[0].CompareTo(f4));
            Assert.Zero(sorted[1].CompareTo(f2));
            Assert.Zero(sorted[2].CompareTo(f3));
            Assert.Zero(sorted[3].CompareTo(f1));            
        }

        [Test]
        public void MergeSortTest()
        {
            var f1 = new FileEntry() { Num = 1, Data = "zzzzz" };
            var f2 = new FileEntry() { Num = 2, Data = "ggg" };
            var f3 = new FileEntry() { Num = 2, Data = "yyy" };
            var f4 = new FileEntry() { Num = 2, Data = "aav" };
            var f5 = new FileEntry() { Num = 1, Data = "KKKKK" };
            var f6 = new FileEntry() { Num = 2, Data = "Lololofff" };
            var f7 = new FileEntry() { Num = 2, Data = "wekly report" };
            var f8 = new FileEntry() { Num = 200, Data = "bababaav" };

            List<FileEntry> entries1 = new List<FileEntry>() { f1, f2, f3, f4 };
            List<FileEntry> entries2 = new List<FileEntry>() { f5, f6, f7, f8, f1, f1 };

            var tempFile1 = Path.GetTempFileName();
            var sortedTempFile1 = Path.GetTempFileName();
            Sorter.WriteList(tempFile1, entries1);
            Sorter.Sort(tempFile1, sortedTempFile1, false);

            var tempFile2 = Path.GetTempFileName();
            var sortedTempFile2 = Path.GetTempFileName();
            Sorter.WriteList(tempFile2, entries2);
            Sorter.Sort(tempFile2, sortedTempFile2, false);

            var resultFile = Path.GetTempFileName();

            var files = new List<string>() { sortedTempFile1, sortedTempFile2 };
            Sorter.MergeTheChunks(files, resultFile, 10, 100);

            // sort
            var sorted = File.ReadAllLines(resultFile).Select(l => FileEntry.Parse(l)).ToList();

            // clean
            File.Delete(tempFile1);
            File.Delete(tempFile2);
            File.Delete(sortedTempFile1);
            File.Delete(sortedTempFile2);
            File.Delete(resultFile);

            // assert
            Assert.AreEqual(sorted.Count, 10);

            Assert.Zero(sorted[0].CompareTo(f4));
            Assert.Zero(sorted[1].CompareTo(f8));
            Assert.Zero(sorted[2].CompareTo(f2));
            Assert.Zero(sorted[3].CompareTo(f5));
            Assert.Zero(sorted[4].CompareTo(f6));
            Assert.Zero(sorted[5].CompareTo(f7));
            Assert.Zero(sorted[6].CompareTo(f3));
            Assert.Zero(sorted[7].CompareTo(f1));
            Assert.Zero(sorted[8].CompareTo(f1));
            Assert.Zero(sorted[9].CompareTo(f1));
        }

        [Test]
        public void EmptySort()
        {
            var f1 = new FileEntry() { Num = 1, Data = "zzzzz" };
            var f2 = new FileEntry() { Num = 2, Data = "ggg" };
            var f3 = new FileEntry() { Num = 2, Data = "yyy" };

            List<FileEntry> entries1 = new List<FileEntry>() { f1, f2, f3 };

            var tempFile1 = Path.GetTempFileName();
            var sortedTempFile1 = Path.GetTempFileName();
            Sorter.WriteList(tempFile1, entries1);
            Sorter.Sort(tempFile1, sortedTempFile1, false);

            var sortedTempFile2 = Path.GetTempFileName();
            var resultFile = Path.GetTempFileName();

            var files = new List<string>() { sortedTempFile1, sortedTempFile2 };
            Sorter.MergeTheChunksWithEmptyFiles(files, resultFile, 10, 100);

            // sort
            var sorted = File.ReadAllLines(resultFile).Select(l => FileEntry.Parse(l)).ToList();

            // clean
            File.Delete(tempFile1);
            File.Delete(sortedTempFile1);
            File.Delete(sortedTempFile2);
            File.Delete(resultFile);

            // assert
            Assert.AreEqual(sorted.Count, 3);
            Assert.Zero(sorted[0].CompareTo(f2));
            Assert.Zero(sorted[1].CompareTo(f3));
            Assert.Zero(sorted[2].CompareTo(f1));
        }
    }
}
