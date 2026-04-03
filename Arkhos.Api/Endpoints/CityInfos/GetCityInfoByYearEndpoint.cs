using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.CityInfo;
using Arkhos.Core.Requests.CityInfos;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.CityInfos;

public class GetCityInfoByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/{year}", handleAsync)
    .WithName("CityInfos: Get By Year")
    .WithSummary("Pega o cityinfos pelo ano.")
    .Produces<Response<ICollection<CityInfoMapDto>>>();

    public static async Task<IResult> handleAsync(
        int year, 
        [FromServices] ICityInfosHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetCityInfosByYearRequest
        {
            Year = year,
            Limit = limit ?? null
            
        };

        var result = await handler.GetByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}