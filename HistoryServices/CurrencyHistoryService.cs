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
    [ScopedService(typeof(IHistoryService<CurrencyHistoryData>))]
    public class CurrencyHistoryService : HistoryServiceBase<CurrencyHistoryData>
    {
        private readonly IHttpClientService _httpClientService;
        private readonly CurrencySourceService _currencySourceService;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly HistoryOptions<Currency> _config;

        public CurrencyHistoryService(
            ICacheProvider<CurrencyHistoryData> cacheProvider,
            ILogger<CurrencyHistoryService> logger,
            IHttpClientService httpClientService,
            CurrencySourceService currencySourceService,
            SecurityStaticDataService staticDataService,
            IOptions<HistoryOptions<Currency>> config) : base(cacheProvider, logger)
        {
            _httpClientService = httpClientService;
            _currencySourceService = currencySourceService;
            _staticDataService = staticDataService;
            _config = config.Value;
        }

        protected override async Task<CurrencyHistoryData> LoadDataAsync(string secId, DateTime day)
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
                ["limit"] = "10",
            });

            CurrencyHistoryData result = null;

            try
            {
                var data = await _httpClientService.Get<CurrencyHistoryData[]>(url, _config.ObjectPath);
                result = data.FirstOrDefault(d => d.WAPRICE != null);
            }
            catch (ServiceResponseException)
            {
            }

            return result;
        }

        protected override async Task<IEnumerable<Task<IEnumerable<CurrencyHistoryData>>>> LoadAllDataAsync()
        {
            var secIds = await _currencySourceService.GetKeys();
            var boardgroups = await _staticDataService.GetBoardgroups(secIds);
            return boardgroups.SelectMany(b => b.Securities.Select(secId => Get(b.Id, secId)));
        }

        private async Task<IEnumerable<CurrencyHistoryData>> Get(int boardgroupId, string secId)
        {
            var result = new List<CurrencyHistoryData>();
            var from = DateTime.MinValue;
            var data = Array.Empty<CurrencyHistoryData>();
            do
            {
                var url = _config.PrepareUrl(boardgroupId, secId, new Dictionary<string, string>
                {
                    ["from"] = from.ToString("yyyy-MM-dd"),
                    ["limit"] = "100",
                });

                try
                {
                    data = await _httpClientService.Get<CurrencyHistoryData[]>(url, _config.ObjectPath);
                    result.AddRange(data.Where(d => d.WAPRICE != null));
                    if (data.Length > 0)
                    {
                        from = data.Max(h => h.TRADEDATE).AddDays(1);
                    }
                }
                catch (ServiceResponseException ex)
                {
                    Logger.LogError(
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
