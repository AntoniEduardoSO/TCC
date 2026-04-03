using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolInfraValues;
using Arkhos.Core.Requests.SchoolInfraValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolInfraValues;

public class GetSchoolInfraValuesWellbeingByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/wellbeing/{year}", HandleAsync)
    .WithName("SchoolInfraValues: Get Wellbeing and Convivencie By Year")
    .WithSummary("Pega o bem-estar e convivência escolar por ano.")
    .Produces<Response<ICollection<SchoolInfraValuesWellbeingDto>>>();


    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolInfraValuesHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolInfraValuesWellbeingByYearRequest
        {
            Year = year,
            Limit = limit ?? null
        };

        var result = await handler.GetWellbeingByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}