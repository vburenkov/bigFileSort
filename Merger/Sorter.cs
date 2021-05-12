using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Merger
{
    public class Sorter
    {
        private string filePath;
        private long fileSize;

        private int linesPerChunk;
        private string workingDir;
        private string resultFile;

        List<string> chunkFilePaths = new List<string>();
        List<string> sortedFilePaths = new List<string>();

        private long totalLines;
        private int averageLineSize;

        public Sorter(string filePath, int linesPerChunk, string workingDir)
        {
            this.filePath = filePath;
            this.fileSize = (new FileInfo(filePath)).Length;
            this.linesPerChunk = linesPerChunk;
            this.workingDir = workingDir;
            this.resultFile = Path.Combine(workingDir, "RESULT.txt");
        }

        public static void Sort(string initialPath, string sortedPath, bool deleteInitial)
        {
            var lines = File.ReadAllLines(initialPath);
            var entries = lines.Select(l => FileEntry.Parse(l)).ToList();
            var ordered = entries.OrderBy(e => e).ToList();
            WriteList(sortedPath, ordered);
            if (deleteInitial) File.Delete(initialPath);
        }

        public static void WriteList(string path, IEnumerable<FileEntry> entries)
        {
            using (StreamWriter writer = new StreamWriter(path))
            {
                foreach (var e in entries)
                {
                    writer.WriteLine(e.ToString());
                }
            }
        }

        public static void WriteList(StreamWriter writer, IEnumerable<FileEntry> entries)
        {
            foreach (var e in entries)
            {
                writer.WriteLine(e.ToString());
            }        
        }

        public static void WriteTheRest(StreamWriter writer, StreamReader reader)
        {
            do
            {
                writer.WriteLine(reader.ReadLine());
            }
            while (!reader.EndOfStream);
        }

        public void StartSimpleSort()
        {
            TimerHelper.WithTimer(() => Split(), nameof(Split));
            TimerHelper.WithTimer(() => InitialSort(), nameof(InitialSort));
            TimerHelper.WithTimer(() => MergeTheChunks(sortedFilePaths, resultFile, totalLines, averageLineSize), 
                nameof(MergeTheChunks));
        }

        public void StartBlockSort()
        {
            TimerHelper.WithTimer(() => Split(), nameof(Split));
            TimerHelper.WithTimer(() => InitialSortUsingBlock(), nameof(InitialSortUsingBlock));
            TimerHelper.WithTimer(() => MergeUsingBinaryBlocks(this.sortedFilePaths, this.workingDir, this.resultFile),
                nameof(MergeUsingBinaryBlocks));
        }

        public void InitialSort()
        {
            List<Task> sortingTasks = new List<Task>();

            foreach (var path in chunkFilePaths)
            {
                var sortedPath = Path.Combine(workingDir, $"s_{Path.GetFileName(path)}");
                sortedFilePaths.Add(sortedPath);               
                Task t = new Task(() => Sort(path, sortedPath, true));
                sortingTasks.Add(t);
                t.Start();
            }

            Task.WhenAll(sortingTasks).Wait();
        }

        public void InitialSortUsingBlock()
        {
            ManualResetEvent initialSortEnd = new ManualResetEvent(false);
            int sortedFilesCount = 0;
            object sortedFilesLock = new object();

            var options = new ExecutionDataflowBlockOptions 
            {
                MaxDegreeOfParallelism = 100,                
                BoundedCapacity = -1
            };

            var action = new ActionBlock<Tuple<string, string>>(input =>
            {
                Sort(input.Item1, input.Item2, true);
                lock (sortedFilesLock)
                {
                    sortedFilesCount++;
                    if (sortedFilesCount == chunkFilePaths.Count)
                    {
                        initialSortEnd.Set();
                    }
                }
            }, options);

            foreach (var path in chunkFilePaths)
            {
                var sortedPath = Path.Combine(workingDir, $"s_{Path.GetFileName(path)}");
                sortedFilePaths.Add(sortedPath);
                action.Post(new Tuple<string, string>(path, sortedPath));
            }

            initialSortEnd.WaitOne();
        }

        private static BatchBlock<string> GetBinaryBatch()
        {
            return new BatchBlock<string>(2);
        }

        private static ActionBlock<string> GetFinalPrintBlock(string resultFile, ManualResetEvent sortingEnd)
        {
            return new ActionBlock<string>((s) =>
            {
                File.Move(s, resultFile);
                Console.WriteLine($"Sorted file {resultFile}");
                sortingEnd.Set();
            });
        }

        private static TransformBlock<IEnumerable<string>, string> GetTransform(int step=0)
        {
            return new TransformBlock<IEnumerable<string>, string>((paths) =>
            {
                string merged = Path.Combine(Path.GetDirectoryName(paths.First()), $"step_{step}_{Guid.NewGuid().ToString()}");
                MergeTheChunksWithEmptyFiles(paths.ToList(), merged, 0, 0);
                return merged;
            },
            new ExecutionDataflowBlockOptions()
            {
                MaxDegreeOfParallelism = 100,
                BoundedCapacity = -1
            });
        }

        private static Tuple<BatchBlock<string>, TransformBlock<IEnumerable<string>, string>> BuildFilterIteration(int ordinal)
        {
            var step1 = GetBinaryBatch();
            var step2 = GetTransform(ordinal);
            step1.LinkTo(step2, new DataflowLinkOptions()
            {
                PropagateCompletion = true
            });

            return new Tuple<BatchBlock<string>, TransformBlock<IEnumerable<string>, string>> (step1, step2);
        }

        private static Tuple<int,int> GetClosestPowerOf2(int count)
        {
            int res = 1;
            int power = 0;

            while (res < count)
            {
                res *= 2;
                power++;
            }

            return new Tuple<int, int>(res, power);
        }

        private static void CreateEmptyFile(string filename)
        {
            File.Create(filename).Dispose();
        }

        private static long GetFileSize(string filename)
        {
            return (new FileInfo(filename)).Length;
        }

        public static void MergeUsingBinaryBlocks(List<string> sorted, string workingDir, string resultFile)
        {
            var desired = GetClosestPowerOf2(sorted.Count);
            var neededFiles = desired.Item1 - sorted.Count;      
            var flowOptions = new DataflowLinkOptions() { PropagateCompletion = true };

            // add empty files
            for (int i = 0; i < neededFiles; i++)
            {
                var newFilePath = Path.Combine(workingDir, $"empty_{Guid.NewGuid()}");
                CreateEmptyFile(newFilePath);
                sorted.Add(newFilePath);                
            }

            var current = BuildFilterIteration(0);
            var startBuffer = current.Item1;

            for (int i = 1; i < desired.Item2;  i++)
            {
                var temp = BuildFilterIteration(i+1);
                current.Item2.LinkTo(temp.Item1, flowOptions);
                current = temp;
            }

            // add final block
            ManualResetEvent sortingEnd = new ManualResetEvent(false);
            var finalBlock = GetFinalPrintBlock(resultFile, sortingEnd);
            current.Item2.LinkTo(finalBlock, flowOptions);

            // post files
            foreach (var p in sorted)
            {
                startBuffer.Post(p);
            }

            sortingEnd.WaitOne();
            startBuffer.Complete();
        }

        public static void MergeTheChunksWithEmptyFiles(List<string> paths,
                                                        string output,
                                                        long totalLines,
                                                        int averageLineSize)
        {
            // remove empty files
            var empty = paths.Where(p => GetFileSize(p) == 0).ToList();
            var nonEmpty = paths.Except(empty).ToList();

            // everything is empty
            if (empty.Count == paths.Count)
            {
                empty.ForEach(f => File.Delete(f));
                File.Create(output);
                return;
            }

            // only one file with data - preserve it
            if (nonEmpty.Count == 1)
            {
                empty.ForEach(f => File.Delete(f));
                File.Delete(output);
                File.Move(nonEmpty.First(), output);
                return;
            }

            empty.ForEach(f => File.Delete(f));
            MergeTheChunks(nonEmpty, output, totalLines, averageLineSize);
            return;
        }

        public static void MergeTheChunks(List<string> paths, 
                                          string output,
                                          long totalLines,
                                          int averageLineSize)
        {
            

            int chunks = paths.Count;
            bool reportPogress = totalLines != 0 && averageLineSize != 0;            
            int maxusage = 1024 * 1024 * 1024; 
            int buffersize = maxusage / chunks; 
            double recordoverhead = 8; 
            int bufferlen = reportPogress == true ? (int)(buffersize / averageLineSize / recordoverhead) : 300;            

            // Open the files
            StreamReader[] readers = new StreamReader[chunks];
            for (int i = 0; i < chunks; i++)
            {
                readers[i] = new StreamReader(paths[i]);
            }

            // Make the queues
            Queue<FileEntry>[] queues = new Queue<FileEntry>[chunks];
            for (int i = 0; i < chunks; i++)
            {
                queues[i] = new Queue<FileEntry>(bufferlen);
            }

            // Load the queues
            for (int i = 0; i < chunks; i++)
            {
                LoadQueue(queues[i], readers[i], bufferlen);
            }

            // merge
            using (StreamWriter sw = new StreamWriter(output))
            {
                bool done = false;
                int lowest_index, j, progress = 0;
                FileEntry lowest_value;
                while (!done)
                {
                    // Report the progress
                    if (reportPogress && ++progress % 5000 == 0)
                    {
                        Console.Write("{0:f2}%\r", 100.0 * progress / totalLines);
                    }

                    // Find the chunk with the lowest value
                    lowest_index = -1;
                    lowest_value = null;

                    for (j = 0; j < chunks; j++)
                    {
                        if (queues[j] != null)
                        {
                            if (lowest_index < 0 || queues[j].Peek().CompareTo(lowest_value) < 0)
                            {
                                lowest_index = j;
                                lowest_value = queues[j].Peek();
                            }
                        }
                    }

                    // Was nothing found in any queue? We must be done then.
                    if (lowest_index == -1)
                    {
                        done = true; break;
                    }

                    // Output it
                    sw.WriteLine(lowest_value);

                    // Remove from queue
                    queues[lowest_index].Dequeue();

                    // Have we emptied the queue? Top it up
                    if (queues[lowest_index].Count == 0)
                    {
                        LoadQueue(queues[lowest_index], readers[lowest_index], bufferlen);

                        // Was there nothing left to read?
                        if (queues[lowest_index].Count == 0)
                        {
                            queues[lowest_index] = null;
                        }
                    }
                }
            }

            // Close and delete the files
            for (int i = 0; i < chunks; i++)
            {
                readers[i].Close();
                File.Delete(paths[i]);
            }
        }

        static void LoadQueue(Queue<FileEntry> queue, StreamReader file, int records)
        {
            for (int i = 0; i < records; i++)
            {
                if (file.Peek() < 0) break;
                queue.Enqueue(FileEntry.Parse(file.ReadLine()));
            }
        }     

        public void Split()
        {
            if (!Directory.Exists(workingDir))
            {
                Directory.CreateDirectory(workingDir);
            }

            Int64 linesCopied = 0;
            bool allLinesProcessed = false;
            StreamWriter streamWriter = null;

            using (StreamReader reader = new StreamReader(filePath))
            {
                do
                {
                    if (streamWriter == null)
                    {
                        var filePath = Path.Combine(workingDir, $"{linesCopied}-{linesCopied + linesPerChunk}");
                        streamWriter = new StreamWriter(filePath);
                        chunkFilePaths.Add(filePath);
                    }

                    var line = reader.ReadLine();

                    // end of file
                    if (line == null)
                    {
                        allLinesProcessed = true;
                        streamWriter.Close();
                        streamWriter.Dispose();
                        streamWriter = null;
                    }
                    else
                    {
                        // copy to another file
                        streamWriter.WriteLine(line);
                        linesCopied++;

                        // chunk is full
                        if (linesCopied % linesPerChunk == 0)
                        {
                            streamWriter.Close();
                            streamWriter.Dispose();
                            streamWriter = null;
                        }
                    }
                }
                while (!allLinesProcessed);
            }

            this.totalLines = linesCopied;
            this.averageLineSize = (int)(this.fileSize / this.totalLines);
        }
    }
}
