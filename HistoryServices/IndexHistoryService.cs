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
    [ScopedService(typeof(IHistoryService<IndexHistoryData>))]
    public class IndexHistoryService : HistoryServiceBase<IndexHistoryData>
    {
        private readonly IHttpClientService _httpClientService;
        private readonly HistoryOptions<Index> _config;

        public IndexHistoryService(
            ICacheProvider<IndexHistoryData> cacheProvider,
            IHttpClientService httpClientService,
            IOptions<HistoryOptions<Index>> config,
            ILogger<IndexHistoryService> logger) : base(cacheProvider, logger)
        {
            _httpClientService = httpClientService;
            _config = config.Value;
        }

        protected override Task<IEnumerable<Task<IEnumerable<IndexHistoryData>>>> LoadAllDataAsync()
        {
            var boardgroups = new[] {
                new Boardgroup
                {
                    Id = 9,
                    Securities = new List<string> { "IMOEX" },
                },
            };
            return Task.FromResult(boardgroups.Select(GetByBoardgroup));
        }

        protected override async Task<IndexHistoryData> LoadDataAsync(string secId, DateTime day)
        {
            var boardgroupId = 9;
            string date = day.Date.ToString("yyyy-MM-dd");
            var url = _config.PrepareUrl(boardgroupId, secId, new Dictionary<string, string>
            {
                ["sort_order"] = "desc",
                ["till"] = date,
                ["limit"] = "1",
            });

            IndexHistoryData result = null;

            try
            {
                var data = await _httpClientService.Get<IndexHistoryData[]>(url, _config.ObjectPath);
                result = data.FirstOrDefault();
            }
            catch (ServiceResponseException)
            {
            }

            return result;
        }

        private async Task<IEnumerable<IndexHistoryData>> GetByBoardgroup(Boardgroup bg)
        {
            var result = new List<IndexHistoryData>();
            var from = DateTime.MinValue;
            var secId = bg.Securities.First();
            var data = Array.Empty<IndexHistoryData>();
            do
            {
                var url = _config.PrepareUrl(bg.Id, secId, new Dictionary<string, string>
                {
                    ["from"] = from.ToString("yyyy-MM-dd"),
                    ["limit"] = "100",
                });

                try
                {
                    data = await _httpClientService.Get<IndexHistoryData[]>(url, _config.ObjectPath);
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
