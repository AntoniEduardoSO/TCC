using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolInfraValues;

public class GetRegionInfraSummaryEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/region", HandleAsync)
            .WithName("SchoolInfraValues: Get Region Summary")
            .WithSummary("Resumo de infraestrutura por região (médias).")
            .Produces<Response<RegionInfraSummaryDto>>();

    public static async Task<IResult> HandleAsync(
        [FromServices] ISchoolInfraValuesHandler handler,
        [FromQuery] int year = 2024,
        [FromQuery] int? mesorregiaoId = null,
        [FromQuery] int? municipioId = null,
        [FromQuery] int? dependencia = null)
    {
        var request = new GetRegionSummaryRequest {
            Year = year, MesorregiaoId = mesorregiaoId, MunicipioId = municipioId, Dependencia = dependencia
        };

        var result = await handler.GetRegionSummaryAsync(request);
        return result.IsSuccess ? TypedResults.Ok(result) : TypedResults.BadRequest(result);
    }
}