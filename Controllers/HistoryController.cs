using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Api.Exceptions;
using Moex.Marketplace.Ignite.Models;
using Moex.Marketplace.Ignite.Models.Interfaces;
using Moex.Marketplace.Ignite.Services.Interfaces;
using Newtonsoft.Json;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class HistoryController : Controller
    {
        private readonly IHistoryService<ShareHistoryData> _shareHistoryService;
        private readonly IHistoryService<BondHistoryData> _bondHistoryService;
        private readonly IHistoryService<CurrencyHistoryData> _currencyHistoryService;
        private readonly IHistoryService<FuturesHistoryData> _futuresHistoryService;
        private readonly IHistoryService<IndexHistoryData> _indexHistoryService;
        private readonly IDataService<TimeStampHistoryData> _timeStampHistoryDataService;

        public HistoryController(
            IHistoryService<ShareHistoryData> shareHistoryService,
            IHistoryService<BondHistoryData> bondHistoryService,
            IHistoryService<CurrencyHistoryData> currencyHistoryService,
            IHistoryService<FuturesHistoryData> futuresHistoryService,
            IHistoryService<IndexHistoryData> indexHistoryService,
            IDataService<TimeStampHistoryData> timeStampHistoryDataService)
        {
            _shareHistoryService = shareHistoryService;
            _bondHistoryService = bondHistoryService;
            _currencyHistoryService = currencyHistoryService;
            _futuresHistoryService = futuresHistoryService;
            _timeStampHistoryDataService = timeStampHistoryDataService;
            _indexHistoryService = indexHistoryService;
        }

        [HttpGet("tenminutely/{secid}")]
        [ResponseCache(Duration = 60)]
        public async Task<IActionResult> GetHourly(string secid)
        {
            if (string.IsNullOrWhiteSpace(secid))
            {
                throw new BadRequestException(nameof(secid));
            }

            var result = (await _timeStampHistoryDataService.FindAsync(secid))
                .OrderBy(data => data.DateTime);
            return Json(result);
        }

        [HttpGet("{secid}/{from?}")]
        [ResponseCache(Duration = 300, VaryByQueryKeys = new[] { "*" })]
        public async Task<IActionResult> Get(string secid, DateTime? from)
        {
            if (string.IsNullOrWhiteSpace(secid))
            {
                throw new BadRequestException(nameof(secid));
            }

            if (!from.HasValue)
            {
                from = DateTime.Now.Date.AddYears(-1);
            }

            var now = DateTime.Now;
            
            async Task<IEnumerable<IHistoryItem>> GetHistoryData<T>(IHistoryService<T> service)
            {
                var data = await service.GetData(secid, from, now);
                return data.Cast<IHistoryItem>();
            }
            
            var result = (await Task.WhenAll(
                GetHistoryData(_shareHistoryService),
                GetHistoryData(_bondHistoryService),
                GetHistoryData(_currencyHistoryService),
                GetHistoryData(_futuresHistoryService),
                GetHistoryData(_indexHistoryService)
            )).SelectMany(_ => _);

            var settings = new JsonSerializerSettings
            {
                DateFormatString = "yyyy'-'MM'-'dd"
            };

            return Json(result, settings);
        }
    }
}
