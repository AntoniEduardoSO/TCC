using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetStudentWithSeriesByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/students/{year}", HandleAsync)
    .WithName("SchoolEnrollValues: Get Students Series By Year")
    .WithSummary("Pega os alunos por serie pelo ano.")
    .Produces<Response<ICollection<SchoolEnrollValuesStudentsDto>>>();


    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetStudentsWithSeriesByYearRequest
        {
            Year = year,
            Limit = limit ?? null
            
        };

        var result = await handler.GetStudentsWithSeriesByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}