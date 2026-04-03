using Arkhos.Api.Common.Api;
using Arkhos.Api.Endpoints.CityInfos;
using Arkhos.Api.Endpoints.SchoolEnrollValues;
using Arkhos.Api.Endpoints.SchoolInfos;
using Arkhos.Api.Endpoints.SchoolInfraValues;
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

        endpoints.MapGroup("v1/cityinfo")
            .WithTags("CityInfo")
            .MapEndpoint<GetCityInfoByYearEndpoint>();

        endpoints.MapGroup("v1/schoolinfo")
            .WithTags("SchoolInfo")
            .MapEndpoint<GetSchoolInfoByYearEndpoint>();

        endpoints.MapGroup("v1/schoolrating")
        .WithTags("SchoolRating")
            .MapEndpoint<GetSchoolRatingSpendingByYearEndpoint>()
            .MapEndpoint<GetSchoolRatingDropoutByYearEndpoint>();

        endpoints.MapGroup("v1/schoolenrollvalues")
            .WithTags("SchoolEnrollValues")
            .MapEndpoint<GetStudentWithSeriesByYearEndpoint>()
            .MapEndpoint<GetTeachersWithSeriesByYearEndpoint>();

        endpoints.MapGroup("v1/schoolinfravalues")
            .WithTags("SchoolInfraValues")
            .MapEndpoint<GetSchoolInfraValuesPedagogicalRoomsByYearEndpoint>();
    }

    private static IEndpointRouteBuilder MapEndpoint<TEndpoint>(this IEndpointRouteBuilder app)
        where TEndpoint : IEndpoint
    {
        TEndpoint.Map(app);
        return app;
    }
}