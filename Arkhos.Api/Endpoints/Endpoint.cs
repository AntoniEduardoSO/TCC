using Arkhos.Api.Common.Api;
using Arkhos.Api.Endpoints.SchoolInfos;
using Arkhos.Api.Endpoints.SchoolRatings;

namespace Arkhos.Api.Endpoints;
public static class Endpoint
{
    public static void MapEndpoints(this WebApplication app)
    {
        var endpoints = app
            .MapGroup("");

        endpoints.MapGroup("/")
            .WithTags("Health Check")
            .MapGet("/", () => new { message = "OK" });

        endpoints.MapGroup("v1/schoolinfo")
            .MapEndpoint<GetSchoolInfoByYearEndpoint>();

        endpoints.MapGroup("v1/schoolrating")
            .MapEndpoint<GetSchoolRatingByYearEndpoint>();
    }

    private static IEndpointRouteBuilder MapEndpoint<TEndpoint>(this IEndpointRouteBuilder app)
        where TEndpoint : IEndpoint
    {
        TEndpoint.Map(app);
        return app;
    }
}