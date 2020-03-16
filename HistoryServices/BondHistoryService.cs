using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moex.Marketplace.Ignite.Exceptions;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Services.Attributes;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Moex.Marketplace.Ignite.Services.SourceServices;

namespace Moex.Marketplace.Ignite.Services.HistoryServices
{
    [ScopedService(typeof(IHistoryService<BondHistoryData>))]
    public class BondHistoryService : HistoryServiceBase<BondHistoryData>
    {
        private static readonly int[] _validBoardgroupIds = new[] { 7, 58 };

        private readonly IHttpClientService _httpClientService;
        private readonly BondSourceService _bondSourceService;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly HistoryOptions<Bond> _config;

        public BondHistoryService(
            ICacheProvider<BondHistoryData> cacheProvider,
            ILogger<BondHistoryService> logger,
            IHttpClientService httpClientService,
            BondSourceService bondSourceService,
            SecurityStaticDataService staticDataService,
            IOptions<HistoryOptions<Bond>> config) : base(cacheProvider, logger)
        {
            _httpClientService = httpClientService;
            _bondSourceService = bondSourceService;
            _staticDataService = staticDataService;
            _config = config.Value;
        }

        protected override async Task<BondHistoryData> LoadDataAsync(string secId, DateTime day)
        {
            var bg = await _staticDataService.GetBoardgroup(secId);
            if (bg == null)
            {
                return null;
            }

            string date = day.ToString("yyyy-MM-dd");
            var url = _config.PrepareUrl(bg.Id, secId, new Dictionary<string, string>
            {
                ["sort_order"] = "desc",
                ["till"] = date,
                ["limit"] = "1",
            });

            try
            {
                var data = await _httpClientService.Get<BondHistoryData[]>(url, _config.ObjectPath);
                return data.FirstOrDefault();
            }
            catch (ServiceResponseException)
            {
            }

            return null;
        }

        protected override async Task<IEnumerable<Task<IEnumerable<BondHistoryData>>>> LoadAllDataAsync()
        {
            var secIds = await _bondSourceService.GetKeys();
            return _validBoardgroupIds.SelectMany(bgId => secIds.Select(secId => Get(bgId, secId)));
        }

        private async Task<IEnumerable<BondHistoryData>> Get(int boardgroupId, string secId)
        {
            var result = new List<BondHistoryData>();
            var from = DateTime.MinValue;
            var data = Array.Empty<BondHistoryData>();
            do
            {
                var url = _config.PrepareUrl(boardgroupId, secId, new Dictionary<string, string>
                {
                    ["from"] = from.ToString("yyyy-MM-dd"),
                    ["limit"] = "100",
                });

                try
                {
                    data = await _httpClientService.Get<BondHistoryData[]>(url, _config.ObjectPath);
                    result.AddRange(data.Where(item => item.CLOSE != null));
                    if (data.Length > 0)
                    {
                        from = data.Max(h => h.TRADEDATE).AddDays(1);
                    }
                }
                catch (ServiceResponseException ex)
                {
                    Logger.LogWarning(
                        ex,
                        "{secId} history data not found. Url: {url}",
                        secId,
                        url);
                    break;
                }
            } while (data.Length > 0);

            return result;
        }
    }
}
