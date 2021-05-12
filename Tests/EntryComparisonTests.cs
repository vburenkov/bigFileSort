using NUnit.Framework;

namespace Merger.Tests
{
    [TestFixture]
    public class EntryComparisonTests
    {
        [Test]
        public void TestComparer()
        {
            FileEntry f1 = new FileEntry()
            {
                Num = 1,
                Data = "aaabbc"
            };

            FileEntry f2 = new FileEntry()
            {
                Num = 100,
                Data = "bbbbfffffssss"
            };

            Assert.AreEqual(f1.CompareTo(f2), -1);
            Assert.AreEqual(f2.CompareTo(f1), 1);
        }

        [Test]
        public void TestComparer2()
        {
            FileEntry f1 = new FileEntry()
            {
                Num = 5,
                Data = "aaabbc"
            };

            FileEntry f2 = new FileEntry()
            {
                Num = 4,
                Data = "aaabbc"
            };

            Assert.AreEqual(f1.CompareTo(f2), 1);
            Assert.AreEqual(f2.CompareTo(f1), -1);
        }

        [Test]
        public void TestComparerEqual()
        {
            FileEntry f1 = new FileEntry()
            {
                Num = 5,
                Data = "aaabbc"
            };

            FileEntry f2 = new FileEntry()
            {
                Num = 5,
                Data = "aaabbc"
            };

            Assert.AreEqual(f1.CompareTo(f2), 0);
            Assert.AreEqual(f2.CompareTo(f1), 0);
        }
    }
}
