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

using Xunit;

namespace TersoSolutions.Jetstream.ServiceBase.Tests
{
    public class JetstreamServiceOptionsTests
    {
        [Fact]
        public void GetEventsLimit_HappyPath()
        {
            const int count = 100;
            var options = new JetstreamServiceOptions();

            options.GetEventsLimit = count;

            Assert.Equal(count, options.GetEventsLimit);
        }
    }
}
