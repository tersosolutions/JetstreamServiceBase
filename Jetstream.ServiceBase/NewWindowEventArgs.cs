using System;
using System.Collections.Generic;
using TersoSolutions.Jetstream.Sdk.Objects.Events;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// Custom EventArgs class for the NewWindow event
    /// </summary>
    public class NewWindowEventArgs : EventArgs
    {
        /// <summary>
        /// Constructor for the custom EventArgs
        /// </summary>
        /// <param name="events"></param>
        /// <exception cref="T:System.ArgumentNullException"><paramref name="events"/> is <see langword="null" />.</exception>
        internal NewWindowEventArgs(IEnumerable<EventDto> events)
        {
            Events = events ?? throw new ArgumentNullException(nameof(events));
        }

        /// <summary>
        /// Ordered window of events received
        /// </summary>
        public IEnumerable<EventDto> Events { get; }
    }
}
