using Arkhos.Api.Common.Api;
using Arkhos.Core.Handlers;
using Arkhos.Core.Models.Dto.SchoolRating;
using Arkhos.Core.Requests.SchoolRatings;
using Arkhos.Core.Responses;
using Microsoft.AspNetCore.Mvc;

namespace Arkhos.Api.Endpoints.SchoolRatings;

public class GetSchoolRatingMapByYearEndpoint : IEndpoint
{
    public static void Map(IEndpointRouteBuilder app)
    => app.MapGet("/ratings/{year}", HandleAsync)
        .WithName("SchoolRatings: Get ratings By Year")
        .WithSummary("Pega o schoolratings pelo ano.")
        .Produces<Response<ICollection<SchoolRatingMapDto>>>();
    public static async Task<IResult> HandleAsync(
        int year, 
        [FromServices] ISchoolRatingsHandler handler,
        [FromQuery] int? limit = null)
    {
        var request = new GetSchoolRatingMapByYearRequest
        {
            Year = year
        };

        var result = await handler.GetRatingByYearAsync(request);

        return result.IsSuccess
            ? TypedResults.Ok(result)
            : TypedResults.BadRequest(result);
    }
}