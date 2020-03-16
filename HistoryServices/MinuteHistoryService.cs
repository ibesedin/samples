using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moex.Marketplace.Ignite.Exceptions;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Interfaces;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Services.Attributes;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Moex.Marketplace.Ignite.Services.SourceServices;
using Newtonsoft.Json.Linq;

namespace Moex.Marketplace.Ignite.Services.HistoryServices
{
    [ScopedService(typeof(IDataService<TimeStampHistoryData>))]
    public class MinuteHistoryService : IDataService<TimeStampHistoryData>
    {
        class TypedBoardgroup : Boardgroup
        {
            public string Type { get; set; }

            public TypedBoardgroup(string type, Boardgroup bg)
            {
                Type = type;
                Id = bg.Id;
                Securities = bg.Securities;
            }

            public IEnumerable<TypedSecurity> ToTypedSecurities()
            {
                return Securities.Select(sec => new TypedSecurity
                {
                    Type = Type,
                    BoardgroupId = Id,
                    SecId = sec,
                });
            }
        }

        class TypedSecurity
        {
            public string Type { get; set; }
            public int BoardgroupId { get; set; }
            public string SecId { get; set; }
        }

        private readonly ILogger _logger;
        private readonly IHttpClientService _getService;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly ShareSourceService _shareSourceService;
        private readonly BondSourceService _bondSourceService;
        private readonly EtfSourceService _etfSourceService;
        private readonly CurrencySourceService _currencySourceService;
        private readonly MinuteHistoryOptions _conf;
        private ICacheProvider<TimeStampHistoryData> _cache;

        public MinuteHistoryService(
            IHttpClientService getService,
            SecurityStaticDataService staticDataService,
            ShareSourceService shareSourceService,
            BondSourceService bondSourceService,
            EtfSourceService etfSourceService,
            CurrencySourceService currencySourceService,
            ICacheProvider<TimeStampHistoryData> cache,
            IOptions<MinuteHistoryOptions> conf,
            ILogger<MinuteHistoryService> logger)
        {
            _logger = logger;
            _getService = getService;
            _staticDataService = staticDataService;
            _shareSourceService = shareSourceService;
            _bondSourceService = bondSourceService;
            _etfSourceService = etfSourceService;
            _currencySourceService = currencySourceService;
            _conf = conf.Value;
            _cache = cache;
        }

        public Task<IEnumerable<TimeStampHistoryData>> FindAsync(string secId)
        {
            return _cache.FindAsync($"{nameof(TimeStampHistoryData.SecId)}:{secId}");
        }

        public Task<IEnumerable<TimeStampHistoryData>> GetAllAsync()
        {
            return _cache.GetAllAsync();
        }

        public Task Remove(string[] keys)
        {
            return _cache.RemoveAllAsync(keys);
        }

        public async Task Update()
        {
            _logger.LogInformation("Update started");

            var yesterday = DateTime.Now.AddDays(-1);
            var earlier = DateTime.Now.AddDays(-4);

            var typedSecurities = (await GetBoardgroups())
                .SelectMany(bg => bg.ToTypedSecurities())
                .ToList();

            var updateTasks = typedSecurities.Select(async (item) =>
            {
                var items = await GetDataForDate(item, yesterday);
                if (!items.Any())
                {
                    items = await GetDataForDate(item, earlier);
                }

                var first = items.FirstOrDefault();
                if (first != null)
                {
                    var existing = (await FindAsync(first.SecId))
                        .Select(h => h.GetKey())
                        .ToList();
                    var removed = existing.Except(items.Select(h => h.GetKey())).ToList();
                    if (removed.Any())
                    {
                        await _cache.RemoveAllAsync(removed);
                        _logger.LogInformation(
                            "Removed {removedCount} items, from {firstKey} to {lastKey}",
                            removed.Count,
                            removed.First(),
                            removed.Last());
                    }

                    var dict = items.ToDictionary(s => s.GetKey());
                    if (dict.Any())
                    {
                        await _cache.PutAllAsync(dict);
                        _logger.LogInformation(
                            "Updated {updatedCount} items, from {firstKey} to {lastKey}",
                            dict.Count,
                            dict.First().Key,
                            dict.Last().Key);
                    }
                }
            });

            await Task.WhenAll(updateTasks);
            _logger.LogInformation("Update finished");
        }

        private async Task<IEnumerable<TypedBoardgroup>> GetBoardgroups()
        {
            var tasks = new[]
            {
                GetTypedBoardgroup(_shareSourceService, "share"),
                GetTypedBoardgroup(_bondSourceService, "bond"),
                GetTypedBoardgroup(_etfSourceService, "share"),
                GetTypedBoardgroup(_currencySourceService, "currency"),
            };

            return (await Task.WhenAll(tasks))
                .SelectMany(_ => _)
                .Concat(new[]
                {
                    new TypedBoardgroup("index", new Boardgroup
                    {
                        Id = 9,
                        Securities = new List<string> {"IMOEX"}
                    })
                });
        }

        private async Task<IEnumerable<TypedBoardgroup>> GetTypedBoardgroup<T>(SourceServiceBase<T> sourceService, string type)
            where T : class, IDictionaryKey, ILoad, new()
        {
            var keys = await sourceService.GetKeys();
            var boardgroups = await _staticDataService.GetBoardgroups(keys);
            return boardgroups.Select(bg => new TypedBoardgroup(type, bg));
        }

        private async Task<IEnumerable<TimeStampHistoryData>> GetDataForDate(TypedSecurity sec, DateTime from)
        {
            var result = Enumerable.Empty<TimeStampHistoryData>();

            JToken token = null;

            var url = _conf.PrepareUrl(sec.Type, sec.BoardgroupId, sec.SecId, new Dictionary<string, string>
            {
                ["from"] = from.ToString("yyyy-MM-dd"),
            });

            try
            {
                token = await _getService.Get<JToken>(url, _conf.ObjectPath);
            }
            catch (ServiceUnavailableException)
            {
                _logger.LogWarning(
                    "{secId} minute history data not found. Url: {url}",
                    sec.SecId,
                    url);
            }

            if (token != null && token.Type == JTokenType.Array)
            {
                result = token
                    .Children()
                    .Select(t => new TimeStampHistoryData(
                        sec.SecId,
                        t.First.Value<long>(),
                        t.Last.Value<float?>())
                    )
                    .ToList();
            }

            return result;
        }
    }
}
