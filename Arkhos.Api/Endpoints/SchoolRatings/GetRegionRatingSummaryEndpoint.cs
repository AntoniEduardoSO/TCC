using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Requests;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolRatings;

public class GetRegionRatingSummaryEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/region", HandleAsync)
            .WithName("SchoolRatings: Get Region Summary")
            .WithSummary("Resumo de ratings e finanças por região (médias).")
            .Produces<Response<RegionRatingSummaryDto>>();

    public static async Task<IResult> HandleAsync(
        [FromServices] ISchoolRatingsHandler handler,
        [FromQuery] int year = 2024,
        [FromQuery] int? mesorregiaoId = null,
        [FromQuery] int? municipioId = null,
        [FromQuery] int? dependencia = null)
    {
        var request = new GetRegionSummaryRequest {
            Year = year, MesorregiaoId = mesorregiaoId, MunicipioId = municipioId, Dependencia = dependencia
        };

        var result = await handler.GetRegionRatingSummaryAsync(request);
        return result.IsSuccess ? TypedResults.Ok(result) : TypedResults.BadRequest(result);
    }
}