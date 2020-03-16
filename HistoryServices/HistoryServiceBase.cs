using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moex.Marketplace.Ignite.Models.Interfaces;
using Moex.Marketplace.Ignite.Services.Interfaces;

namespace Moex.Marketplace.Ignite.Services.HistoryServices
{
    public abstract class HistoryServiceBase<T> : IHistoryService<T> where T : class, IDictionaryKey, IHistoryItem
    {
        private readonly ICacheProvider<T> _cacheProvider;

        protected readonly ILogger Logger;

        public HistoryServiceBase(
            ICacheProvider<T> provider,
            ILogger logger)
        {
            _cacheProvider = provider;
            Logger = logger;
        }

        public async Task<T> GetData(string secId, DateTime? day)
        {
            var data = await GetData(secId, null, day);
            var result = data.LastOrDefault();

            if (result == null)
            {
                result = day.HasValue ? await LoadDataAsync(secId, day.Value) : null;
                if (result != null)
                {
                    await _cacheProvider.PutAsync(result.GetKey(), result);
                }
                else
                {
                    Logger.LogWarning(
                        "{secId} for {day} data not found",
                        secId,
                        day?.ToString("yyyy-MM-dd"));
                }
            }

            return result;
        }

        public async Task<IEnumerable<T>> GetData(IEnumerable<string> secIds, DateTime? day)
        {
            var data = await GetData(secIds, null, day);
            var puts = (await Task.WhenAll(secIds
                .Where(secId => !data.Any(item => item.SECID == secId))
                .Select(async secId =>
                {
                    if (!day.HasValue)
                    {
                        return null;
                    }

                    var loaded = await LoadDataAsync(secId, day.Value);
                    if (loaded == null)
                    {
                        Logger.LogWarning(
                            "{secId} for {day} data not found",
                            secId,
                            day?.ToString("yyyy-MM-dd"));
                    }

                    return loaded;
                })))
                .Where(item => item != null)
                .ToList();

            if (puts.Any())
            {
                await _cacheProvider.PutAllAsync(puts.ToDictionary(p => p.GetKey()));
            }

            var allData = data.Concat(puts).OrderBy(item => item.TRADEDATETICKS);
            return secIds
                .Select(secId => allData.LastOrDefault(item => item.SECID == secId))
                .Where(item => item != null)
                .ToList();
        }

        public Task<T> GetDataYearAgo(string secId)
        {
            var date = DateTime.Now.AddYears(-1);
            return GetData(secId, date);
        }

        public Task<IEnumerable<T>> GetDataYearAgo(IEnumerable<string> secIds)
        {
            var date = DateTime.Now.AddYears(-1);
            return GetData(secIds, date);
        }

        public async Task<IEnumerable<T>> GetData(string secId, DateTime? from, DateTime? till)
        {
            var sql = new List<string>();
            var args = new List<object>();
            if (!string.IsNullOrEmpty(secId))
            {
                sql.Add($"LOWER({nameof(IHistoryItem.SECID)}) = LOWER(?)");
                args.Add(secId);
            }

            if (from.HasValue)
            {
                sql.Add($"{nameof(IHistoryItem.TRADEDATETICKS)} >= ?");
                args.Add(from.Value.Ticks);
            }

            if (till.HasValue)
            {
                sql.Add($"{nameof(IHistoryItem.TRADEDATETICKS)} <= ?");
                args.Add(till.Value.Ticks);
            }

            return (await _cacheProvider.FindAsync(string.Join(" AND ", sql), args.ToArray()))
                .OrderBy(item => item.TRADEDATETICKS)
                .ToList();
        }

        public async Task<IEnumerable<T>> GetData(IEnumerable<string> secIds, DateTime? from, DateTime? till)
        {
            var sql = new List<string>();
            var args = new List<object>();
            if (secIds.Any())
            {
                var inClause = string.Join(",", secIds.Select(s => "LOWER(?)"));
                sql.Add($"LOWER({nameof(IHistoryItem.SECID)}) IN ({inClause})");
                args.AddRange(secIds);
            }

            if (from.HasValue)
            {
                sql.Add($"{nameof(IHistoryItem.TRADEDATETICKS)} >= ?");
                args.Add(from.Value.Ticks);
            }

            if (till.HasValue)
            {
                sql.Add($"{nameof(IHistoryItem.TRADEDATETICKS)} <= ?");
                args.Add(till.Value.Ticks);
            }

            return (await _cacheProvider.FindAsync(string.Join(" AND ", sql), args.ToArray()))
                .OrderBy(item => item.TRADEDATETICKS)
                .ToList();
        }

        protected abstract Task<T> LoadDataAsync(string secId, DateTime date);
        protected abstract Task<IEnumerable<Task<IEnumerable<T>>>> LoadAllDataAsync();

        public async Task Update()
        {
            Logger.LogInformation("Update all data started");

            var tasks = (await LoadAllDataAsync()).Select(async dataTask =>
            {
                var data = (await dataTask)
                    .Where(item => item != null)
                    .ToDictionary(item => item.GetKey());
                if (data.Any())
                {
                    await _cacheProvider.PutAllAsync(data);
                    Logger.LogInformation(
                        "Updated {dataCount} items, from {firstKey} to {lastKey}",
                        data.Count,
                        data.First().Key,
                        data.Last().Key);
                }
            });

            await Task.WhenAll(tasks);

            Logger.LogInformation("Update all data finished");
        }
    }
}
