using System;
using System.Collections.Generic;
using System.Text;

namespace Merger
{
    public interface IEntryGenerator<T>
    {
        T GetRandomEntry();
    }
}
