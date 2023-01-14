/*
    Copyright 2023 Terso Solutions, Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

using System.Collections.Generic;
using TersoSolutions.Jetstream.Sdk.Objects.Events;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// Object for a Jetstream get events response
    /// </summary>
    public class JetstreamEventsResponse
    {
        /// <summary>
        /// Batch ID
        /// </summary>
        public string BatchId { get; set; }

        /// <summary>
        /// Sorted Jetstream events
        /// </summary>
        public SortedSet<EventDto> Events { get; set; }
    }
}
