using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolInfraValues;

public class GetSchoolInfraDetailEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
        => app.MapGet("/summary/{schoolId:int}", HandleAsync)
            .WithName("SchoolInfraValues: Get School Detail")
            .WithSummary("Detalhes de infraestrutura de uma escola específica.")
            .Produces<Response<SchoolInfraDetailDto>>();

    public static async Task<IResult> HandleAsync(
        int schoolId,
        [FromQuery] int year,
        [FromServices] ISchoolInfraValuesHandler handler)
    {
        var result = await handler.GetSchoolDetailAsync(schoolId, year);
        return result.IsSuccess ? TypedResults.Ok(result) : TypedResults.BadRequest(result);
    }
}