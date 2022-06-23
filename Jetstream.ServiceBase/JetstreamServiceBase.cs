/*
    Copyright 2022 Terso Solutions, Inc.

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

using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using TersoSolutions.Jetstream.Sdk;
using TersoSolutions.Jetstream.Sdk.Objects;
using TersoSolutions.Jetstream.Sdk.Objects.Events;
using TersoSolutions.Jetstream.ServiceBase.OldDto;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// An abstract service class that pops events from Jetstream
    /// </summary>
    public abstract class JetstreamServiceBase : IHostedService, IDisposable
    {

        #region Data

        private const int DefaultGetEventsLimit = 500;
        private Task _eventProcessingTask = Task.CompletedTask;

        private JetstreamServiceOptions _options;
        private Timer _getEventsTimer;
        private bool _isDisposed;

        private readonly IDisposable _optionsChangeToken;
        private readonly ILogger _logger;
        private readonly IJetstreamClient _jetstreamClient;

        #endregion

        /// <summary>
        /// Constructor for the service base
        /// </summary>
        /// <param name="loggerFactory">LoggerFactory object</param>
        /// <param name="jetstreamClient"></param>
        /// <param name="options">Jetstream options</param>
        /// <exception cref="T:System.ArgumentNullException">Logging factory cannot be null <paramref name="loggerFactory"/></exception>
        /// <exception cref="T:System.ArgumentNullException">Options object cannot be null <paramref name="options"/></exception>
        protected JetstreamServiceBase(ILoggerFactory loggerFactory, IJetstreamClient jetstreamClient, IOptionsMonitor<JetstreamServiceOptions> options)
        {
            // Validate input
            if (loggerFactory == null) throw new ArgumentNullException(nameof(loggerFactory), "Logging factory cannot be null");
            if (jetstreamClient == null) throw new ArgumentNullException(nameof(jetstreamClient), "Jetstream client cannot be null");
            if (options == null) throw new ArgumentNullException(nameof(options), "Options object cannot be null");

            // Handles Updates
            _optionsChangeToken = options.OnChange(UpdateOptions);

            // Assign settings
            _logger = loggerFactory.CreateLogger<JetstreamServiceBase>();
            _jetstreamClient = jetstreamClient;
            UpdateOptions(options.CurrentValue);
        }

        /// <summary>
        /// Handles the updating of options
        /// </summary>
        /// <param name="options">Jetstream options object</param>
        private void UpdateOptions(JetstreamServiceOptions options)
        {
            _options = options;
        }

        #region Service Events

        /// <summary>
        /// Entry point to start event processing
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public virtual Task StartAsync(CancellationToken cancellationToken)
        {
            // Start getting events
            _getEventsTimer = new Timer(BeginEventProcessing, cancellationToken, new TimeSpan(0, 1, 0), _options.MessageCheckWindow);

            return Task.CompletedTask;
        }

        /// <summary>
        /// Exit point to stop event processing
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        /// <exception cref="T:System.NullReferenceException">The <see cref="T:System.Runtime.CompilerServices.TaskAwaiter"></see> object was not properly initialized.</exception>
        /// <exception cref="T:System.Threading.Tasks.TaskCanceledException">The task was canceled.</exception>
        /// <exception cref="T:System.Exception">The task completed in a <see cref="F:System.Threading.Tasks.TaskStatus.Faulted"></see> state.</exception>
        public virtual Task StopAsync(CancellationToken cancellationToken)
        {
            // Stop timer
            _getEventsTimer.Change(Timeout.Infinite, 0);

            // Let the event processing finish
            _eventProcessingTask.GetAwaiter().GetResult();

            // Dispose of things
            Dispose();

            return Task.CompletedTask;
        }

        /// <summary>
        /// Dispose of necessary objects
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose of necessary objects
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (_isDisposed) return;

            _getEventsTimer?.Dispose();
            _optionsChangeToken?.Dispose();

            _isDisposed = true;
        }

        #endregion

        #region Jetstream event popping

        /// <summary>
        /// Start async event processing
        /// </summary>
        /// <param name="state">Cancellation token</param>
        private void BeginEventProcessing(object state)
        {
            _logger.LogTrace("Get events execution starting...");

            // Setup cancellation token
            var ct = (CancellationToken)state;

            // Check if cancelled
            if (ct.IsCancellationRequested) return;

            // Use Task for locking
            if (!_eventProcessingTask.IsCompleted) return;

            try
            {
                // Process application events
                _eventProcessingTask = ProcessApplicationAsync();
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing messages");
            }
        }

        /// <summary>
        /// Full get, process, delete events for an application
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// This method must be async in order to thread
        /// </remarks>
        private async Task ProcessApplicationAsync()
        {
            // Explanation of ConfigureAwait https://devblogs.microsoft.com/dotnet/configureawait-faq/
            // Get events first
            var response = await GetJetstreamEvents().ConfigureAwait(false);

            // All done if no events
            if (!response.Events.Any()) return;

            // Process Jetstream events
            await ProcessJetstreamEvents(response.Events).ConfigureAwait(false);

            // Delete the batch now that it's been processed
            await DeleteJetstreamEventsAsync(response.BatchId).ConfigureAwait(false);
        }

        /// <summary>
        /// Get Jetstream events for an application
        /// </summary>
        /// <returns></returns>
        private async Task<JetstreamEventsResponse> GetJetstreamEvents()
        {
            _logger.LogTrace("Starting to retrieve Jetstream events...");

            var batchId = string.Empty;
            var sortedSet = new SortedSet<EventDto>(new EventComparer());

            try
            {
                var response = await _jetstreamClient.GetEventsAsync(_options.GetEventsLimit ?? DefaultGetEventsLimit).ConfigureAwait(false);
                batchId = response.BatchId;

                _logger.LogTrace($"Got {response.Count} events in batch {batchId}");

                // Step through the received events and add them to the sorted set
                foreach (var eventDto in response.Events)
                {
                    if (sortedSet.All(x => x.EventId != eventDto.EventId))
                    {
                        sortedSet.Add(eventDto);
                    }
                }
            }
            catch (JetstreamException e)
            {
                // Log and pass exception to event handler for any processing
                _logger.LogError(JsonConvert.SerializeObject(e));
                await HandleJetstreamGetEventsExceptionAsync(e, batchId).ConfigureAwait(false);
            }

            return new JetstreamEventsResponse
            {
                BatchId = batchId,
                Events = sortedSet
            };
        }

        /// <summary>
        /// Process Jetstream events for an application
        /// </summary>
        /// <param name="eventDtos"></param>
        private async Task ProcessJetstreamEvents(ICollection<EventDto> eventDtos)
        {
            _logger.LogTrace($"Starting to process {eventDtos.Count}");

            // https://stackoverflow.com/a/4233539/5217488
            var eventDelegates = new Dictionary<string, Delegate>
            {
                {"aggregateevent", new Func<AggregateEventDto, Task>(ProcessAggregateEventAsync)},
                {"commandcompletionevent", new Func<CommandCompletionEventDto, Task>(ProcessCommandCompletionEventAsync)},
                {"commandqueuedevent", new Func<CommandQueuedEventDto, Task>(ProcessCommandQueuedEventAsync)},
                {"heartbeatevent", new Func<HeartbeatEventDto, Task>(ProcessHeartbeatEventAsync)},
                {"logentryevent", new Func<LogEntryEventDto, Task>(ProcessLogEntryEventAsync)},
                {"logicaldeviceaddedevent", new Func<EventDto, Task>(ConvertAndProcessLogicalDeviceAddedEventAsync)},
                {"deviceaddedevent", new Func<DeviceAddedEventDto, Task>(ProcessDeviceAddedEventAsync)},
                {"logicaldeviceremovedevent", new Func<EventDto, Task>(ConvertAndProcessLogicalDeviceRemovedEventAsync)},
                {"deviceremovedevent", new Func<DeviceRemovedEventDto, Task>(ProcessDeviceRemovedEventAsync)},
                {"devicemodifiedevent", new Func<DeviceModifiedEventDto, Task>(ProcessDeviceModifiedEventAsync)},
                {"objectevent", new Func<ObjectEventDto, Task>(ProcessObjectEventAsync)},
                {"sensorreadingevent", new Func<SensorReadingEventDto, Task>(ProcessSensorReadingEventAsync)},
                {"aliasaddedevent", new Func<AliasAddedEventDto, Task>(ProcessAliasAddedEventAsync)},
                {"aliasremovedevent", new Func<AliasRemovedEventDto, Task>(ProcessAliasRemovedEventAsync)},
                {"aliasmodifiedevent", new Func<AliasModifiedEventDto, Task>(ProcessAliasModifiedEventAsync)},
                {"devicecredentialsaddedevent", new Func<DeviceCredentialsAddedEventDto, Task>(ProcessDeviceCredentialsAddedEventAsync)},
                {"devicecredentialsremovedevent", new Func<DeviceCredentialsRemovedEventDto, Task>(ProcessDeviceCredentialsRemovedEventAsync)},
                {"devicecredentialsmodifiedevent", new Func<DeviceCredentialsModifiedEventDto, Task>(ProcessDeviceCredentialsModifiedEventAsync)},
                {"businessprocessevent", new Func<BusinessProcessEventDto, Task>(ProcessBusinessProcessEventAsync)},
                {"statusevent", new Func<StatusEventDto, Task>(ProcessStatusEventAsync)}
            };

            // Process the list of events
            foreach (var eventDto in eventDtos)
            {
                var eventType = eventDto.Type.Trim().ToLower();
                var stopwatch = new Stopwatch();
                stopwatch.Start();

                try
                {
                    if (eventDelegates.ContainsKey(eventType))
                    {
                        // Known event type
                        await ((Task)eventDelegates[eventType].DynamicInvoke(eventDto)).ConfigureAwait(false);
                    }
                    else
                    {
                        // Unknown event type
                        await ProcessUnknownMessageAsync(eventDto).ConfigureAwait(false);
                    }
                }
                catch (Exception e)
                {
                    _logger.LogError(e, "Error processing event");
                }

                stopwatch.Stop();

                _logger.LogTrace($"Took {stopwatch.Elapsed} to process {eventType}");
            }
        }

        /// <summary>
        /// Delete a batch of Jetstream events
        /// </summary>
        /// <param name="batchId"></param>
        private async Task DeleteJetstreamEventsAsync(string batchId)
        {
            _logger.LogTrace($"Deleting event batch {batchId}");

            try
            {
                await _jetstreamClient.DeleteEventsAsync(new DeleteEventsDto { BatchId = batchId }).ConfigureAwait(false);
            }
            catch (JetstreamException jsException)
            {
                _logger.LogError(JsonConvert.SerializeObject(jsException));
                await HandleJetstreamDeleteEventsExceptionAsync(jsException, batchId).ConfigureAwait(false);
            }
        }

        #endregion

        #region Jetstream exception handlers

        /// <summary>
        /// Method to do some extra handling when an exception occurs on a Get Events
        /// </summary>
        /// <param name="jsException">Jetstream exception</param>
        /// <param name="batchId">Event batch ID</param>
        protected abstract Task HandleJetstreamGetEventsExceptionAsync(JetstreamException jsException, string batchId);

        /// <summary>
        /// Method to do some extra handling when an exception occurs on a Delete Events
        /// </summary>
        /// <param name="jsException">Jetstream exception</param>
        /// <param name="batchId">Event batch ID</param>
        protected abstract Task HandleJetstreamDeleteEventsExceptionAsync(JetstreamException jsException, string batchId);

        #endregion

        #region Jetstream event handlers

        /// <summary>
        /// Method for processing a new <paramref name="aggregateEvent"/>
        /// </summary>
        /// <param name="aggregateEvent">Deserialized AggregateEvent object</param>
        protected abstract Task ProcessAggregateEventAsync(AggregateEventDto aggregateEvent);

        /// <summary>
        /// Method for processing a new <paramref name="commandCompletionEvent"/>
        /// </summary>
        /// <param name="commandCompletionEvent">Deserialized CommandCompletionEvent object</param>
        protected abstract Task ProcessCommandCompletionEventAsync(CommandCompletionEventDto commandCompletionEvent);

        /// <summary>
        /// Method for processing a new <paramref name="commandQueuedEvent"/>
        /// </summary>
        /// <param name="commandQueuedEvent">Deserialized CommandQueuedEvent object</param>
        protected abstract Task ProcessCommandQueuedEventAsync(CommandQueuedEventDto commandQueuedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="heartbeatEvent"/>
        /// </summary>
        /// <param name="heartbeatEvent">Deserialized HeartbeatEvent object</param>
        protected abstract Task ProcessHeartbeatEventAsync(HeartbeatEventDto heartbeatEvent);

        /// <summary>
        /// Method for processing a new <paramref name="logEntryEvent"/>
        /// </summary>
        /// <param name="logEntryEvent">Deserialized LogEntryEvent object</param>
        protected abstract Task ProcessLogEntryEventAsync(LogEntryEventDto logEntryEvent);

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceAddedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceAddedEvent">Deserialized LogicalDeviceAdded object</param>
        protected abstract Task ProcessLogicalDeviceAddedEventAsync(LogicalDeviceAddedEventDto logicalDeviceAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceAddedEventDto"/>
        /// </summary>
        /// <param name="deviceAddedEventDto">Deserialized LogicalDeviceAddedEvent object</param>
        protected abstract Task ProcessDeviceAddedEventAsync(DeviceAddedEventDto deviceAddedEventDto);

        /// <summary>
        /// Method for processing a new <paramref name="logicalDeviceRemovedEvent"/>
        /// </summary>
        /// <param name="logicalDeviceRemovedEvent">Deserialized LogicalDeviceRemovedEvent object</param>
        protected abstract Task ProcessLogicalDeviceRemovedEventAsync(LogicalDeviceRemovedEventDto logicalDeviceRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceRemovedEvent"/>
        /// </summary>
        /// <param name="deviceRemovedEvent">Deserialized LogicalDeviceRemovedEvent</param>
        protected abstract Task ProcessDeviceRemovedEventAsync(DeviceRemovedEventDto deviceRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceModifiedEvent"/>
        /// </summary>
        /// <param name="deviceModifiedEvent">Deserialized DeviceModifiedEvent object</param>
        protected abstract Task ProcessDeviceModifiedEventAsync(DeviceModifiedEventDto deviceModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="objectEvent"/>
        /// </summary>
        /// <param name="objectEvent">Deserialized ObjectEvent object</param>
        protected abstract Task ProcessObjectEventAsync(ObjectEventDto objectEvent);

        /// <summary>
        /// Method for processing a new <paramref name="sensorReadingEvent"/>
        /// </summary>
        /// <param name="sensorReadingEvent">Deserialized SensorReadingEvent object</param>
        protected abstract Task ProcessSensorReadingEventAsync(SensorReadingEventDto sensorReadingEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasAddedEvent"/>
        /// </summary>
        /// <param name="aliasAddedEvent">Deserialized AliasAddedEvent object</param>
        protected abstract Task ProcessAliasAddedEventAsync(AliasAddedEventDto aliasAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasRemovedEvent"/>
        /// </summary>
        /// <param name="aliasRemovedEvent">Deserialized AliasRemovedEvent object</param>
        protected abstract Task ProcessAliasRemovedEventAsync(AliasRemovedEventDto aliasRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="aliasModifiedEvent"/>
        /// </summary>
        /// <param name="aliasModifiedEvent">Deserialized AliasModifiedEvent object</param>
        protected abstract Task ProcessAliasModifiedEventAsync(AliasModifiedEventDto aliasModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsAddedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsAddedEvent">Deserialized DeviceCredentialsAddedEvent</param>
        protected abstract Task ProcessDeviceCredentialsAddedEventAsync(DeviceCredentialsAddedEventDto deviceCredentialsAddedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsRemovedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsRemovedEvent">Deserialized DeviceCredentialsRemovedEvent</param>
        protected abstract Task ProcessDeviceCredentialsRemovedEventAsync(DeviceCredentialsRemovedEventDto deviceCredentialsRemovedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="deviceCredentialsModifiedEvent"/>
        /// </summary>
        /// <param name="deviceCredentialsModifiedEvent">Deserialized DeviceCredentialsModifiedEvent object</param>
        protected abstract Task ProcessDeviceCredentialsModifiedEventAsync(DeviceCredentialsModifiedEventDto deviceCredentialsModifiedEvent);

        /// <summary>
        /// Method for processing a new <paramref name="businessProcessEventDto"/>
        /// </summary>
        /// <param name="businessProcessEventDto">Deserialized BusinessProcessEventDto object</param>
        protected abstract Task ProcessBusinessProcessEventAsync(BusinessProcessEventDto businessProcessEventDto);

        /// <summary>
        /// Method for processing a new <paramref name="statusEventDto"/>
        /// </summary>
        /// <param name="statusEventDto">Deserialized StatusEventDto object</param>
        /// <returns></returns>
        protected abstract Task ProcessStatusEventAsync(StatusEventDto statusEventDto);

        /// <summary>
        /// Method for processing unknown events
        /// </summary>
        /// <param name="message">The unknown event</param>
        protected abstract Task ProcessUnknownMessageAsync(EventDto message);

        #endregion

        #region Helpers

        /// <summary>
        /// Convert the <paramref name="eventDto"/> to a LogicalDeviceAddedEventDto
        /// </summary>
        /// <param name="eventDto">Event to convert</param>
        /// <returns></returns>
        private Task ConvertAndProcessLogicalDeviceAddedEventAsync(EventDto eventDto)
        {
            // v2 and under event only
            var json = JsonConvert.SerializeObject(eventDto);
            var message = JsonConvert.DeserializeObject<LogicalDeviceAddedEventDto>(json);
            return ProcessLogicalDeviceAddedEventAsync(message);
        }

        /// <summary>
        /// Convert the <paramref name="eventDto"/> to a LogicalDeviceRemovedEventDto
        /// </summary>
        /// <param name="eventDto">Event to convert</param>
        /// <returns></returns>
        private Task ConvertAndProcessLogicalDeviceRemovedEventAsync(EventDto eventDto)
        {
            // v2 and under event only
            var json = JsonConvert.SerializeObject(eventDto);
            var message = JsonConvert.DeserializeObject<LogicalDeviceRemovedEventDto>(json);
            return ProcessLogicalDeviceRemovedEventAsync(message);
        }

        #endregion

    }
}
