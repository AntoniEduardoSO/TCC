using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfo;
using Arkhos.Core.Requests.SchoolInfos;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolInfos;

public class GetSchoolInfoByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/{year}", HandleAsync)
    .WithName("SchoolInfos: Get By Year")
    .WithSummary("Pega o schoolinfos pelo ano.")
    .Produces<Response<ICollection<SchoolInfoMapDto>>>();

    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolInfosHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolInfoByYearRequest
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



