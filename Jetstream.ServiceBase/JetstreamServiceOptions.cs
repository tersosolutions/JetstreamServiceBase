using System;

namespace TersoSolutions.Jetstream.ServiceBase
{
    /// <summary>
    /// Options for the Jetstream service
    /// </summary>
    public class JetstreamServiceOptions
    {
        private Uri _jetstreamUrl;
        private string _userAccessKey;
        private TimeSpan _messageCheckWindow = TimeSpan.FromMinutes(1);
        private int? _getEventsLimit = 500;

        /// <summary>
        /// Jetstream API URL
        /// </summary>
        /// <exception cref="T:System.ArgumentException" accessor="set">Absolute URL is invalid.</exception>
        public Uri JetstreamUrl
        {
            get => _jetstreamUrl;
            set
            {
                if (string.IsNullOrEmpty(value.AbsoluteUri)) throw new ArgumentException(nameof(JetstreamUrl));
                _jetstreamUrl = value;
            }
        }

        /// <summary>
        /// Jetstream user access key
        /// </summary>
        /// <exception cref="T:System.ArgumentNullException" accessor="set">User access key is <see langword="null"/></exception>
        public string UserAccessKey
        {
            get => _userAccessKey;
            set
            {
                if (string.IsNullOrEmpty(value)) throw new ArgumentNullException(nameof(UserAccessKey));
                _userAccessKey = value.Trim();
            }
        }

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
