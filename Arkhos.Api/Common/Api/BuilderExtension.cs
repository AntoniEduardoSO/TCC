using Arkhos.Api.Handlers;
using Arkhos.Core.Handlers;
using Microsoft.OpenApi;

namespace Arkhos.Api.Common.Api;

public static class BuilderExtension
{
    public static void AddDocumentation(this WebApplicationBuilder builder)
    {
        builder.Services.AddOpenApi();
        builder.Services.AddSwaggerGen(x =>
        {
            x.SwaggerDoc("v1", new OpenApiInfo
            {
                Version = "v1",
                Title = "ToDo Arkhos Api ",
                Description = "Um sistema para criação de dashboards para ajuda a gestão educacional com ML preditivos-prescritivos.",
                Contact = new OpenApiContact
                {
                    Name = "Codigo Fonte - Github",
                    Url = new Uri("https://github.com/AntoniEduardoSO/TCC")
                },
            });

            x.CustomSchemaIds(n => n.FullName);
        });
    }

    public static void AddServices(this WebApplicationBuilder builder)
    {
        builder
            .Services
            .AddTransient<ISchoolInfosHandler, SchoolInfosHandler>();
    }
}


