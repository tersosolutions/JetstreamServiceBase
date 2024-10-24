/*
    Copyright 2024 Terso Solutions, Inc.

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

using System;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// Options for the Jetstream service
    /// </summary>
    public class JetstreamServiceOptions
    {
        private TimeSpan _messageCheckWindow = TimeSpan.FromMinutes(1);
        private int? _getEventsLimit = 500;

        /// <summary>
        /// The frequency at which service check Jetstream for Events
        /// </summary>
        /// <exception cref="T:System.ArgumentOutOfRangeException" accessor="set">Message check window must be positive.</exception>
        public TimeSpan MessageCheckWindow
        {
            get => _messageCheckWindow;
            set
            {
                if (value <= TimeSpan.Zero)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), $"{nameof(MessageCheckWindow)} must be positive.");
                }
                _messageCheckWindow = value;
            }
        }

        /// <summary>
        /// The max number of to be returned by a get events call
        /// </summary>
        /// <exception cref="T:System.ArgumentOutOfRangeException" accessor="set">Get events limit must be positive.</exception>
        public int? GetEventsLimit
        {
            get => _getEventsLimit;
            set
            {
                if (value < 0)
                {
                    throw new ArgumentOutOfRangeException(nameof(value), $"{nameof(GetEventsLimit)} must be positive.");
                }
                _getEventsLimit = value;
            }
        }
    }
}
