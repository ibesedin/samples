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

namespace Moex.Marketplace.Ignite.Services.HistoryServices
{
    [ScopedService(typeof(IHistoryService<FuturesHistoryData>))]
    public class FuturesHistoryService : HistoryServiceBase<FuturesHistoryData>
    {
        private readonly IHttpClientService _httpClientService;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly HistoryOptions<Futures> _config;

        public FuturesHistoryService(
            ICacheProvider<FuturesHistoryData> cacheProvider,
            ILogger<FuturesHistoryService> logger,
            IHttpClientService httpClientService,
            SecurityStaticDataService staticDataService,
            IOptions<HistoryOptions<Futures>> config) : base(cacheProvider, logger)
        {
            _httpClientService = httpClientService;
            _staticDataService = staticDataService;
            _config = config.Value;
        }

        protected override async Task<FuturesHistoryData> LoadDataAsync(string secId, DateTime day)
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

            FuturesHistoryData result = null;

            try
            {
                var data = await _httpClientService.Get<FuturesHistoryData[]>(url, _config.ObjectPath);
                result = data.FirstOrDefault();
            }
            catch (ServiceResponseException)
            {
            }

            return result;
        }

        protected override Task<IEnumerable<Task<IEnumerable<FuturesHistoryData>>>> LoadAllDataAsync()
        {
            return Task.FromResult(Enumerable.Empty<Task<IEnumerable<FuturesHistoryData>>>());
        }
    }
}
