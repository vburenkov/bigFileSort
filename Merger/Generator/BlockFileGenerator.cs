using Merger.Generator;
using System;
using System.IO;
using System.Threading.Tasks.Dataflow;

namespace Merger
{
    public class BlockFileGenerator: IFileGenerator<FileEntry>
    { 
        private string pathToSave;
        private Int64 desiredSize;
        private IEntryGenerator<FileEntry> entryGenerator;

        public string Path => pathToSave;

        public BlockFileGenerator(IEntryGenerator<FileEntry> entryGenerator,
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
                var options = new ExecutionDataflowBlockOptions()
                {
                    BoundedCapacity = -1,
                    MaxDegreeOfParallelism = 10,
                    EnsureOrdered = false
                };

                ActionBlock<string> writerBlock = new ActionBlock<string>(s => writer.WriteLine(s), options);
                BufferBlock<string> bufferBlock = new BufferBlock<string>();
                bufferBlock.LinkTo(writerBlock, new DataflowLinkOptions() { PropagateCompletion = true });

                do
                {
                    var entryTxt = entryGenerator.GetRandomEntry().ToString();
                    bufferBlock.Post(entryTxt);
                    writtenBytes += entryTxt.Length + 1;
                }
                while (writtenBytes < desiredSize);

                bufferBlock.Complete();
                writerBlock.Completion.Wait();
            }               
        }
    }
}
