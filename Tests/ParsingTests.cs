using NUnit.Framework;
using System;

namespace Merger.Tests
{
    [TestFixture]
    public class ParsingTests
    {
        [Test]
        public void ParseTest()
        {
            var f = FileEntry.Parse("1. Kolia petia");
            Assert.AreEqual(f.Num, 1);
            Assert.AreEqual(f.Data, "Kolia petia");
        }

        [Test]
        public void ParseTest_LongStr()
        {
            var f = FileEntry.Parse("1345.  some long string    with spaces inside");
            Assert.AreEqual(f.Num, 1345);
            Assert.AreEqual(f.Data, "some long string    with spaces inside");
        }

        [Test]
        public void ParseTest_MultipleDots()
        {
            var f = FileEntry.Parse("12.  some long string.    with dots. inside");
            Assert.AreEqual(f.Num, 12);
            Assert.AreEqual(f.Data, "some long string.    with dots. inside");
        }

        [Test]
        public void ParseTest_Err()
        {
            Assert.Throws<FormatException>(() => FileEntry.Parse(".  fff"));
        }

        [Test]
        public void ParseTest_Err2()
        {
            Assert.Throws<FormatException>(() => FileEntry.Parse("fergfregre  fff"));
        }

        [Test]
        public void ParseTest_Err3()
        {
            Assert.Throws<FormatException>(() => FileEntry.Parse(""));
        }

    }
}
