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
    [ScopedService(typeof(IHistoryService<ShareHistoryData>))]
    public class ShareHistoryService : HistoryServiceBase<ShareHistoryData>
    {
        private readonly IHttpClientService _httpClientService;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly ShareSourceService _shareSourceService;
        private readonly EtfSourceService _etfSourceService;
        private readonly HistoryOptions<Share> _config;

        class UpdatingItem
        {
            public int BoardgroupId { get; private set; }
            public string SecId { get; private set; }

            public UpdatingItem(int boardgroupId, string secId)
            {
                BoardgroupId = boardgroupId;
                SecId = secId;
            }
        }

        public ShareHistoryService(
            ICacheProvider<ShareHistoryData> cacheProvider,
            ILogger<ShareHistoryService> logger,
            IHttpClientService httpClientService,
            ShareSourceService shareSourceService,
            EtfSourceService etfSourceService,
            SecurityStaticDataService staticDataService,
            IOptions<HistoryOptions<Share>> config) : base(cacheProvider, logger)
        {
            _httpClientService = httpClientService;
            _staticDataService = staticDataService;
            _shareSourceService = shareSourceService;
            _etfSourceService = etfSourceService;
            _config = config.Value;
        }

        protected override async Task<IEnumerable<Task<IEnumerable<ShareHistoryData>>>> LoadAllDataAsync()
        {
            var secIds = (await _shareSourceService.GetKeys()).Concat(await _etfSourceService.GetKeys());
            var boardgroups = await _staticDataService.GetBoardgroups(secIds);
            return boardgroups.SelectMany(b => b.Securities.Select(secId => Get(b.Id, secId)));
        }

        protected override async Task<ShareHistoryData> LoadDataAsync(string secId, DateTime day)
        {
            var bg = await _staticDataService.GetBoardgroup(secId);
            if (bg == null)
            {
                return null;
            }

            string date = day.Date.ToString("yyyy-MM-dd");
            var url = _config.PrepareUrl(bg.Id, secId, new Dictionary<string, string>
            {
                ["sort_order"] = "desc",
                ["till"] = date,
                ["limit"] = "1",
            });

            ShareHistoryData result = null;

            try
            {
                var data = await _httpClientService.Get<ShareHistoryData[]>(url, _config.ObjectPath);
                result = data.FirstOrDefault();
            }
            catch (ServiceResponseException)
            {
            }

            return result;
        }

        private async Task<IEnumerable<ShareHistoryData>> Get(int boardgroupId, string secId)
        {
            var result = new List<ShareHistoryData>();
            var from = DateTime.MinValue;
            var data = Array.Empty<ShareHistoryData>();
            do
            {
                var url = _config.PrepareUrl(boardgroupId, secId, new Dictionary<string, string>
                {
                    ["from"] = from.ToString("yyyy-MM-dd"),
                    ["limit"] = "100",
                });

                try
                {
                    data = await _httpClientService.Get<ShareHistoryData[]>(url, _config.ObjectPath);
                    result.AddRange(data);
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
