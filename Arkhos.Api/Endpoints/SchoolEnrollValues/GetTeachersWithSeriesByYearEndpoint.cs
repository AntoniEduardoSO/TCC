using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolEnrollValues;
using Arkhos.Core.Requests.SchoolEnrollValues;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolEnrollValues;

public class GetTeachersWithSeriesByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/teachers/{year}", HandleAsync)
        .WithName("SchoolEnrollValues: Get Teachers Series By Year")
        .WithSummary("Pega os professores por serie pelo ano.")
        .Produces<Response<ICollection<SchoolEnrollValuesTeachersDto>>>();
    
    public static async Task<IResult> HandleAsync(
        int year,
        [FromServices] ISchoolEnrollValuesHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetTeachersWithSeriesByYearRequest
        {
            Year = year,
            Limit = limit ?? null
            
        };

        var result = await handler.GetTeachersWithSeriesByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}