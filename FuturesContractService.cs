using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Options;
using Moex.Marketplace.Ignite.Services.Attributes;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Newtonsoft.Json.Linq;

namespace Moex.Marketplace.Ignite.Services
{
    [ScopedService]
    public class FuturesContractService
    {
        private const int MaxHistoryBackwardStepCount = 30;
        private const int CurrentContractGapInDays = 3;
        private const int FuturesBoardgroupId = 45;

        private readonly IHttpClientService _getService;
        private readonly ICacheProvider<string> _stringCacheProvider;
        private readonly ICacheProvider<FuturesContract> _contractCacheProvider;
        private readonly MarketOptions<Futures> _marketConf;
        private readonly HistoryOptions<Futures> _historyConf;
        private readonly SecurityStaticDataService _staticDataService;
        private readonly ILogger _logger;
        private readonly TimeSpan _futuresContractHistoryOffset;

        public FuturesContractService(
            IHttpClientService getService,
            ICacheProvider<string> stringCacheProvider,
            ICacheProvider<FuturesContract> contractCacheProvider,
            IOptions<MarketOptions<Futures>> marketConf,
            IOptions<HistoryOptions<Futures>> historyConf,
            SecurityStaticDataService staticDataService,
            ILogger<FuturesContractService> logger)
        {
            _getService = getService;
            _stringCacheProvider = stringCacheProvider;
            _contractCacheProvider = contractCacheProvider;
            _marketConf = marketConf.Value;
            _historyConf = historyConf.Value;
            _staticDataService = staticDataService;
            _logger = logger;
            _futuresContractHistoryOffset = TimeSpan.FromDays(365 * 3 + 30);
        }

        public async Task<string> GetContractForDate(string secType, DateTime dt)
        {
            var date = dt.Date;
            var cacheKey = GetCacheKey(secType, date);
            var contract = await _contractCacheProvider.GetAsync(cacheKey);
            if (contract != null)
            {
                return contract.SecId;
            }

            var assetCode = await GetAssetCode(secType);
            if (string.IsNullOrEmpty(assetCode))
            {
                _logger.LogWarning("Error get assetcode for sectype {secType}", secType);
                return null;
            }

            SecurityStaticData security;
            if (date < DateTime.Now.Date)
            {
                var secIds = await GetSecIdsByAssetCode(assetCode, date);
                var data = await _staticDataService.Get(secIds);
                security = data
                    .Where(s => s.LastDelDate > date.AddDays(3))
                    .OrderBy(s => s.LastDelDate)
                    .FirstOrDefault();
            }
            else
            {
                security = await GetCurrentSecurity(secType, date);
            }

            if (security?.SecId != null)
            {
                await _contractCacheProvider.PutAsync(cacheKey, new FuturesContract(security, secType));
                _logger.LogInformation(
                    "Update futures contact {cacheKey} = {securitySecId}",
                    cacheKey,
                    security.SecId);
            }

            return security?.SecId;
        }

        public async Task UpdateAllContracts(IEnumerable<string> secTypes)
        {
            _logger.LogInformation("Update all futures contracts started");
            await Task.WhenAll(secTypes.Select(UpdateContracts));
            _logger.LogInformation("Update all futures contracts finished");
        }

        private async Task<string> GetAssetCode(string secType)
        {
            var assetCodeCacheKey = $"ASSETCODE-{secType}";
            var assetCode = await _stringCacheProvider.GetAsync(assetCodeCacheKey);
            if (assetCode == null)
            {
                var security = await GetCurrentSecurity(secType, DateTime.Today);
                assetCode = security?.AssetCode;
                if (assetCode != null)
                {
                    await _stringCacheProvider.PutAsync(assetCodeCacheKey, assetCode);
                }
            }

            return assetCode;
        }

        private string GetCacheKey(string secType, DateTime date) => $"{secType}-{date.ToString("yyyy-MM-dd")}";

        private async Task<SecurityStaticData> GetCurrentSecurity(string secType, DateTime date)
        {
            var url = _marketConf.PrepareUrl(FuturesBoardgroupId, null, new Dictionary<string, string>
            {
                ["sort_column"] = "LASTDELDATE",
                ["sort_order"] = "asc",
                ["sectypes"] = secType,
            });
            var token = await _getService.Get<JToken>(url, "$.[1].securities[1]");
            var staticDataBySecType = token
                .Children()
                .Select(t => new SecurityStaticData(t.Value<string>("SECID"))
                {
                    AssetCode = t.Value<string>("ASSETCODE"),
                    LastDelDate = t.Value<DateTime>("LASTDELDATE")
                })
                .OrderBy(s => s.LastDelDate)
                .ToList();

            var skipped = staticDataBySecType.Where(s => s.LastDelDate > date.AddDays(CurrentContractGapInDays)).ToList();

            if (!skipped.Any())
            {
                _logger.LogWarning(
                    "After {days} days there are no contacts for sectype {secType}. Will try to return current contract.",
                    CurrentContractGapInDays,
                    secType);
            }

            return (skipped.Any() ? skipped : staticDataBySecType).FirstOrDefault();
        }

        private async Task<IEnumerable<string>> GetSecIdsByAssetCode(string assetCode, DateTime date)
        {
            var maxAttemptsCacheKey = $"SECIDSNOTFOUND-{assetCode}-{date.ToString("yyyy-MM-dd")}";
            var value = await _stringCacheProvider.GetAsync(maxAttemptsCacheKey);
            if (value != null)
            {
                return new string[0];
            }

            var children = Enumerable.Empty<JToken>();

            var reqDate = date.Date;
            int count = 0;
            do
            {
                var url = _historyConf.PrepareUrl(FuturesBoardgroupId, null, new Dictionary<string, string>
                {
                    ["assetcode"] = assetCode,
                    ["date"] = reqDate.ToString("yyyy-MM-dd"),
                });
                var token = await _getService.Get<JToken>(url, _historyConf.ObjectPath);
                children = token.Children().ToList();

                reqDate = reqDate.AddDays(-1);
                count++;
            } while (children.Count() < 1 && count < MaxHistoryBackwardStepCount);

            if (count >= MaxHistoryBackwardStepCount)
            {
                _logger.LogWarning(
                    "Error get history contract data for assetcode {assetCode} till {date}. Max attempt count reached.",
                    assetCode,
                    date.ToString("yyyy-MM-dd"));
                await _stringCacheProvider.PutAsync(maxAttemptsCacheKey, string.Empty);
            }

            return children.Select(t => t.Value<string>("SECID"));
        }

        private async Task UpdateContracts(string secType)
        {
            var assetCode = await GetAssetCode(secType);
            if (assetCode == null)
            {
                _logger.LogError(
                    "Cannot evaluate assetcode for sectype {secType}. Contracts update skipped.",
                    secType);
                return;
            }

            var date = DateTime.Today.AddDays(-1);
            var futuresSecIds = Enumerable.Empty<string>();
            IEnumerable<string> secIds;
            do
            {
                secIds = (await GetSecIdsByAssetCode(assetCode, date)).Where(id => id != null);
                futuresSecIds = futuresSecIds.Concat(secIds).Distinct();
                date = date.AddMonths(-1);

            } while (secIds.Any() && (DateTime.Today - date) < _futuresContractHistoryOffset);

            var securities = (await _staticDataService.Get(futuresSecIds))
                .OrderBy(s => s.LastDelDate)
                .ToList();

            var items = securities
                .Select(s => new FuturesContract(s, secType))
                .ToDictionary(item => item.GetKey());
            await _contractCacheProvider.PutAllAsync(items);

            _logger.LogInformation(
                "Updated {securitiesCount} contracts for sectype {secType}",
                securities.Count(),
                secType);
        }

        public async Task<FuturesContract[]> GetContracts(string secType)
        {
            var result = await _contractCacheProvider.FindAsync($"{nameof(FuturesContract.SecType)}:{secType}");
            return result
                .Distinct(FuturesContract.ByLastDelDate)
                .OrderBy(s => s.LastDelDate)
                .ToArray();
        }
    }
}
