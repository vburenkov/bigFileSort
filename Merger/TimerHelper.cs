using System;
using System.Diagnostics;

namespace Merger
{
    public class TimerHelper
    {
        public static void WithTimer(Action a, string name)
        {
            var sw = new Stopwatch();
            sw.Start();
            a();
            sw.Stop();
            Console.WriteLine($"{name}: " + Math.Truncate(sw.Elapsed.TotalMilliseconds) + " ms");
        }
    }
}
