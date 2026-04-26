using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetRegionEnrollmentSummaryEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/region", HandleAsync)
        .WithName("SchoolEnrollValues: Get Region Summary By Filter")
        .WithSummary("Pega o resumo consolidado de matrículas e escolas de uma região (Meso, Micro, Município ou Estado).")
        .Produces<Response<RegionEnrollmentSummaryDto>>();

    public static async Task<IResult> HandleAsync(
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int year = 2024,
        [FromQuery] int? depedencia = null,
        [FromQuery] int? mesorregiaoId = null,
        [FromQuery] int? microrregiaoId = null,
        [FromQuery] int? municipioId = null
        )
    {
        // Monta o Request com todos os filtros opcionais
        var request = new GetRegionEnrollmentSummaryByFilterRequest
        {
            Year = year,
            Depedencia = depedencia,
            MesorregiaoId = mesorregiaoId,
            MicrorregiaoId = microrregiaoId,
            MunicipioId = municipioId
        };

        // Passa o Request para o nosso novo método focado na Região
        var result = await handler.GetRegionEnrollmentSummaryByFilterAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}