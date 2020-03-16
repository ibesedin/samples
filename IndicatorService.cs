using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using System.Timers;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moex.IssPlusWebSocketClient;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Services.Attributes;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Newtonsoft.Json.Linq;
using Timer = System.Timers.Timer;

namespace Moex.Marketplace.Ignite.Services
{
    [SingletonService] //NB: SingletonService
    public class IndicatorService : IDisposable
    {
        private static TimeSpan ResubscribeInterval = TimeSpan.FromMinutes(5);

        private class IndicatorChange
        {
            public float? Value { get; set; }
            public string Trend { get; set; }
            public DateTime? UpdateTime { get; set; }

            public bool IsAnythingValid() => IsValueValid() || Trend != null || IsUpdateTimeValid();

            public bool IsValueValid() => Value.HasValue && Value != 0;

            public bool IsUpdateTimeValid() => UpdateTime.HasValue && UpdateTime != DateTime.MinValue;
        }

        private static readonly string[] _messageMandatoryFields;
        private static readonly TimeZoneInfo _russianTimeZone;

        private readonly IndicatorsConfiguration _conf;
        private readonly ILogger<IndicatorService> _logger;
        private readonly ICacheProvider<Indicator> _cache;
        private readonly WebSocketClient _client;
        private readonly ConcurrentDictionary<string, DateTime> _tickerSubStamps;
        private readonly ConcurrentDictionary<string, Indicator> _throttleUpdateDict;
        private readonly Timer _tickerResubTimer;
        private readonly Timer _throttleUpdateTimer;

        private IEnumerable<Subscription> _subscriptions;

        public bool Paused { get; private set; }
        
        static IndicatorService()
        {
            _messageMandatoryFields = new[]
            {
                "TICKER",
            };

            var zoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? "Russian Standard Time"
                : "Europe/Moscow";
            _russianTimeZone = TimeZoneInfo.FindSystemTimeZoneById(zoneId);
        }

        public IndicatorService(
            ICacheProvider<Indicator> cache,
            IOptions<IndicatorsConfiguration> conf,
            WebSocketClient client,
            ILogger<IndicatorService> logger)
        {
            _logger = logger;
            _cache = cache;
            _conf = conf.Value;
            _client = client;
            _tickerSubStamps = new ConcurrentDictionary<string, DateTime>();
            _tickerResubTimer = new Timer(TimeSpan.FromMinutes(1).TotalMilliseconds);
            _tickerResubTimer.Elapsed += TickerResubTimerElapsed;
            _throttleUpdateDict = new ConcurrentDictionary<string, Indicator>();
            _throttleUpdateTimer = new Timer(TimeSpan.FromSeconds(10).TotalMilliseconds);
            _throttleUpdateTimer.Elapsed += ThrottleUpdateTimerElapsed;

            CreateSubscriptions();
            Paused = false;
        }

        public async Task<IEnumerable<Indicator>> GetAll()
        {
            return (await _cache.GetAllAsync()).OrderBy(ind => ind.Index).ToList();
        }

        private void CreateSubscriptions()
        {
            _subscriptions = _conf.Items
                .Select(item => new Subscription(item.Ticker, HandleMessage))
                .ToArray();
        }

        public async Task StartUpdating()
        {
            foreach (var sub in _subscriptions)
            {
                await AddSubscription(sub);
            }

            if (!_client.Connected)
            {
                await _client.Connect();
                _logger.LogInformation("Connected");
            }

            _tickerResubTimer.Start();
            _throttleUpdateTimer.Start();

            _logger.LogInformation($"Update started");
        }

        public async Task Pause()
        {
            if (!Paused)
            {
                _tickerResubTimer.Stop();
                _throttleUpdateTimer.Stop();
                Paused = true;
                foreach (var sub in _subscriptions)
                {
                    await RemoveSubscription(sub);
                }
                _logger.LogInformation("Paused");
            }
        }

        public async Task Unpause()
        {
            if (Paused)
            {
                foreach (var sub in _subscriptions)
                {
                    sub.RenewId();
                    await AddSubscription(sub);
                }
                Paused = false;
                _tickerResubTimer.Start();
                _throttleUpdateTimer.Start();
                _logger.LogInformation("Unpaused");
            }
        }

        private async Task AddSubscription(Subscription sub)
        {
            await _client.Add(sub);
            RefreshStamp(sub.Ticker);
            _logger.LogInformation("Subscription to {ticker} added", sub.Ticker);
        }

        private async Task RemoveSubscription(Subscription sub)
        {
            await _client.Remove(sub);
            _logger.LogInformation("Subscription to {ticker} removed", sub.Ticker);
        }

        private async void TickerResubTimerElapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                _logger.LogInformation($"{nameof(TickerResubTimerElapsed)} started");
                var now = DateTime.Now;
                foreach (var kvp in _tickerSubStamps.Where(kvp => (now - kvp.Value) > ResubscribeInterval))
                {
                    var sub = _subscriptions.First(s => s.Ticker == kvp.Key);
                    await Resubscribe(sub);
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, nameof(TickerResubTimerElapsed));
            }
        }

        private async void ThrottleUpdateTimerElapsed(object sender, ElapsedEventArgs e)
        {
            try
            {
                // thread-safe get snapshot
                var list = _throttleUpdateDict.Values.ToList();
                if (list.Count > 0)
                {
                    await _cache.PutAllAsync(list.ToDictionary(ind => ind.GetKey()));
                }
            }
            catch (Exception exception)
            {
                _logger.LogError(exception, nameof(TickerResubTimerElapsed));
            }
        }

        private async Task Resubscribe(Subscription sub)
        {
            _logger.LogInformation("Resubscribe to {ticker}", sub.Ticker);
            await RemoveSubscription(sub);
            sub.RenewId();
            await AddSubscription(sub);
        }

        private void RefreshStamp(string ticker)
        {
            _tickerSubStamps[ticker] = DateTime.Now;
            _logger.LogDebug(
                "Set stamp {tickerStamp} for {ticker}",
                _tickerSubStamps[ticker],
                ticker);
        }

        private IndicatorConfiguration GetConfigurationById(SubscriptionMessage message)
        {
            if (message.GetColumnValue("TICKER") is string ticker)
            {
                var secid = ticker.Split('.').Last();
                return _conf.Items.FirstOrDefault(ind => ind.Id == secid);
            }

            return null;
        }

        private async Task HandleMessage(object sender, SubscriptionEventArgs e)
        {
            var message = e.Message;
            if (message?.Data == null || message.Data.Length == 0)
            {
                return;
            }

            if (message.Columns.Intersect(_messageMandatoryFields).Count() != _messageMandatoryFields.Length)
            {
                _logger.LogWarning("Message dropped because of mandatory field(s) absence");
                return;
            }

            try
            {
                var id = GetConfigurationById(message);
                if (id == null)
                {
                    _logger.LogWarning("Configuration for {message} not found", message);
                    return;
                }

                _logger.LogDebug("Parsed id: {id}", id.Id);

                if (message.Properties.Type == SubscriptionMessageType.Updates)
                {
                    var change = Parse(message);
                    if (change.IsAnythingValid())
                    {
                        _throttleUpdateDict.TryGetValue(id.Id, out var indicator);
                        var changed = false;
                        if (indicator == null)
                        {
                            indicator = new Indicator()
                            {
                                Id = id.Id,
                                Index = id.Index,
                                Name = id.Name,
                            };
                            changed = true;
                        }
                        if (change.IsValueValid() && indicator.Value != change.Value)
                        {
                            indicator.Value = change.Value;
                            changed = true;
                        }
                        if (change.Trend != null && indicator.Trend != change.Trend)
                        {
                            indicator.Trend = change.Trend;
                            changed = true;
                        }
                        if (change.IsUpdateTimeValid() && indicator.UpdateTime != change.UpdateTime)
                        {
                            indicator.UpdateTime = change.UpdateTime;
                            changed = true;
                        }

                        if (changed)
                        {
                            RefreshStamp(id.Ticker);
                            _throttleUpdateDict[indicator.GetKey()] = indicator;
                            _logger.LogInformation("Updates {indicator}", indicator);
                        }
                        else
                        {
                            _logger.LogDebug("No changes");
                        }
                    }
                }
                else if (message.Properties.Type == SubscriptionMessageType.Snapshot)
                {
                    var change = Parse(message);
                    if (change.IsValueValid())
                    {
                        var indicator = new Indicator()
                        {
                            Id = id.Id,
                            Index = id.Index,
                            Name = id.Name,
                            Value = change.Value,
                            Trend = change.Trend,
                            UpdateTime = change.UpdateTime,
                        };

                        RefreshStamp(id.Ticker);
                        _throttleUpdateDict[indicator.GetKey()] = indicator;
                        _logger.LogInformation("Snapshot {indicator}", indicator);
                    }
                    else
                    {
                        _logger.LogError("Skipped snapshot message with invalid value: {message}", message);
                    }
                }
                else
                {
                    RefreshStamp(id.Ticker);
                    _throttleUpdateDict.TryRemove(id.Id, out _);
                    await _cache.RemoveAsync(id.Id);
                    _logger.LogInformation("Remove {id}", id.Id);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Invalid indicator update");
            }
        }

        private IndicatorChange Parse(SubscriptionMessage message)
        {
            var result = new IndicatorChange();

            if (message.Columns.Contains("LAST") && message.GetColumnValue("LAST") is JArray last)
            {
                var value = last[0].Value<float?>();
                if (!value.HasValue || value == 0)
                {
                    _logger.LogWarning("Invalid value {value} in message", value?.ToString() ?? "null");
                }
                else
                {
                    result.Value = value;
                    _logger.LogDebug("Parsed value: {value}", value);
                }
            }

            if (message.Columns.Contains("CHANGE") && message.GetColumnValue("CHANGE") is JArray change)
            {
                result.Trend = GetTrendText(change[0].Value<float?>());
                _logger.LogDebug("Parsed trend: {trend}", result.Trend);
            }

            if (message.Columns.Contains("TIME") && message.GetColumnValue("TIME") is string time && time != null)
            {
                var updateTime = DateTime.Parse($"{DateTime.Now.ToString("yyyy-MM-dd")} {time}");
                updateTime = TimeZoneInfo.ConvertTimeToUtc(updateTime, _russianTimeZone);

                if (updateTime > DateTime.Now.ToUniversalTime())
                {
                    updateTime = updateTime.AddDays(-1);
                }

                updateTime = TimeZoneInfo.ConvertTimeFromUtc(updateTime, _russianTimeZone);

                result.UpdateTime = updateTime;
                _logger.LogDebug("Parsed updateTime: {updateTime}", updateTime);
            }

            return result;
        }

        private string GetTrendText(float? val)
        {
            if (!val.HasValue)
            {
                return null;
            }

            switch (Math.Sign(val.Value))
            {
                case -1: return "negative";
                case 1: return "positive";
                default: return string.Empty;
            }
        }

        public void Dispose()
        {
            foreach (var sub in _subscriptions)
            {
                sub.OnMessageReceived -= HandleMessage;
            }

            _tickerResubTimer.Elapsed -= TickerResubTimerElapsed;
            _tickerResubTimer.Stop();
            _tickerResubTimer.Dispose();

            _throttleUpdateTimer.Elapsed -= ThrottleUpdateTimerElapsed;
            _throttleUpdateTimer.Stop();
            _throttleUpdateTimer.Dispose();
        }
    }
}
