using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models;
using Arkhos.Core.Models.Dto;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolRatings;

public class GetSchoolRatingByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/{year}", HandleAsync)
        .WithName("SchoolRatings: Get By Year")
        .WithSummary("Pega o schoolratings pelo ano.")
        .Produces<Response<ICollection<SchoolRatingSpendingDto>>>();


    public static async Task<IResult> HandleAsync(
        int year, 
        [FromServices] ISchoolRatingsHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolRatingByYearRequest
        {
            Year = year
        };

        var result = await handler.GetByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}