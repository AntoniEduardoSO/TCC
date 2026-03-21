using Arkhos.Api.Data;
using Arkhos.Api.Handlers;
using Arkhos.Core;
using Arkhos.Core.Handlers;
using Microsoft.EntityFrameworkCore;
using Microsoft.OpenApi;

namespace Arkhos.Api.Common.Api;

public static class BuilderExtension
{
    public static void AddConfiguration(this WebApplicationBuilder builder)
    {
        Configuration.ConnectionString = 
            builder
            .Configuration
            .GetConnectionString("DefaultConnection")
        ?? string.Empty;

        Configuration.BackendUrl = builder.Configuration.GetValue<string>("BackendUrl") ?? string.Empty;
        Configuration.FrontendUrl = builder.Configuration.GetValue<string>("FrontendUrl") ?? string.Empty;
    }
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

    public static void AddDataContexts(this WebApplicationBuilder builder)
    {

        builder.Services.AddDbContext<AppDbContext>(
            x =>
            {
                x.UseNpgsql(Configuration.ConnectionString);
            }
        );
    }

    public static void AddCrossOrigin(this WebApplicationBuilder builder)
    {
        builder.Services.AddCors(
            options => options.AddPolicy(
                ApiConfiguration.CorsPolicyName,
                policy => policy
                    .WithOrigins([
                        Configuration.BackendUrl,
                        Configuration.FrontendUrl
                    ])
                    .AllowAnyMethod()
                    .AllowAnyHeader()
                    .AllowCredentials()
            )
        );
    }

}