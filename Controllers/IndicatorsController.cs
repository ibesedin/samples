using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Moex.Marketplace.Ignite.Services;

namespace Moex.Marketplace.Api.Controllers
{
    [ServiceFilter(typeof(ApiExceptionFilter))]
    [Produces("application/json")]
    [Route("api/[controller]")]
    public class IndicatorsController : Controller
    {
        private readonly IndicatorService _indicatorService;

        public IndicatorsController(IndicatorService indicatorService)
        {
            _indicatorService = indicatorService;
        }

        [HttpGet]
        [ResponseCache(Duration = 60)]
        public async Task<IActionResult> Get()
        {
            return Json(await _indicatorService.GetAll());
        }
    }
}
