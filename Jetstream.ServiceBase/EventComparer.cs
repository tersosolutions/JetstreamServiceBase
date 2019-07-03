using System.Collections.Generic;
using TersoSolutions.Jetstream.Sdk.Objects.Events;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// Sort events by time
    /// </summary>
    public class EventComparer : IComparer<EventDto>
    {
        /// <summary>
        /// Compare the times
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <returns></returns>
        public int Compare(EventDto x, EventDto y)
        {
            return x.EventTime < y.EventTime ? -1 : 1;
        }
    }
}
