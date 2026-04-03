using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetSchoolEnrollValuesGovernanceByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/governance/{year}", HandleAsync)
    .WithName("SchoolEnrollValues: Get Governance By Year")
    .WithSummary("Pega as entidades de governança por serie pelo ano.")
    .Produces<Response<ICollection<SchoolEnrollValuesGovernanceDto>>>();

    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolEnrollValuesGovernanceByYearRequest
        {
            Year = year,
            Limit = limit ?? null
            
        };

        var result = await handler.GetGovernanceByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}