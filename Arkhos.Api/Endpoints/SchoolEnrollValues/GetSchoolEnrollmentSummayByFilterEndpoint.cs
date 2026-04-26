using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetSchoolEnrollmentDetailEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        // Passamos o ID da escola direto na rota para manter o padrão RESTful
        => app.MapGet("/summary/{schoolId}", HandleAsync)
        .WithName("SchoolEnrollValues: Get Detail By Filter")
        .WithSummary("Pega os detalhes de matrícula da escola com os filtros de ano e dependência administrativa.")
        .Produces<Response<SchoolEnrollmentDetailDto>>();

    public static async Task<IResult> HandleAsync(
        [FromRoute] int schoolId,
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int year = 2024
        )
    {

        var result = await handler.GetSchoolEnrollmentDetailByFilterAsync(schoolId, year);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}