using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetRegionSummariesListEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/list", HandleAsync)
        .WithName("SchoolEnrollValues: Get Region Summaries List")
        .WithSummary("Retorna uma lista de resumos das sub-regiões para alimentar o cache de hover do mapa.")
        .Produces<Response<ICollection<RegionEnrollmentSummaryDto>>>();

    public static async Task<IResult> HandleAsync(
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int year = 2024,
        [FromQuery] string? parentLevel = null,
        [FromQuery] int? parentId = null)
    {
        var request = new GetRegionSummariesListRequest
        {
            Year = year,
            ParentLevel = parentLevel,
            ParentId = parentId
        };

        var result = await handler.GetRegionSummariesListAsync(request);

        return result.IsSuccess ? TypedResults.Ok(result) : TypedResults.BadRequest(result);
    }
}