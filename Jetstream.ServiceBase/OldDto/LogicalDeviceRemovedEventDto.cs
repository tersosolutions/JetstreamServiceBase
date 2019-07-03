using TersoSolutions.Jetstream.Sdk.Objects.Events;

namespace TersoSolutions.Jetstream.ServiceBase.OldDto
{
    /// <summary>
    /// The Logical Device Removed Event data sent when a device is 
    /// removed from an application using the DELETE HTTP verb
    /// on v2 Devices/{LogicalDeviceId}
    /// </summary>
    public class LogicalDeviceRemovedEventDto : EventDto
    {
        /// <summary>
        /// The friendly name of the device
        /// </summary>
        public string LogicalDeviceId { get; set; }
    }
}
