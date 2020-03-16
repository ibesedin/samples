using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Api.Exceptions;
using Moex.Marketplace.Ignite.Services;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class DividendsController : Controller
    {
        private readonly DividendCacheService _dividendCacheService;

        public DividendsController(DividendCacheService dividendCacheService)
        {
            _dividendCacheService = dividendCacheService;
        }

        [HttpGet("{secid}")]
        [ResponseCache(Duration = 3600)]
        public async Task<IActionResult> Get(string secid)
        {
            if (string.IsNullOrWhiteSpace(secid))
            {
                throw new BadRequestException(nameof(secid));
            }

            return Json((await _dividendCacheService.GetByIdAsync(secid)).OrderBy(d => d.Date));
        }
    }
}
