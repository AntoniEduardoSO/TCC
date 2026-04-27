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
        Configuration.ConnectionString = builder.Configuration.GetConnectionString("DefaultConnection") ?? string.Empty;

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
            .AddScoped<ISchoolInfosHandler, SchoolInfosHandler>();

        builder
            .Services
            .AddScoped<ISchoolRatingsHandler, SchoolRatingsHandler>();

        builder
            .Services
            .AddScoped<ISchoolEnrollValuesHandler, SchoolEnrollValuesHandler>();

        builder
            .Services
            .AddScoped<ICityInfosHandler, CityInfosHandler>();

        builder
            .Services
            .AddScoped<ISchoolInfraValuesHandler, SchoolInfraValuesHandler>();

        builder
            .Services
            .AddScoped<ITargetInsightsHandler, TargetInsightsHandler>();
    }

    public static void AddDataContexts(this WebApplicationBuilder builder)
    {
        var isProduction = builder.Environment.IsProduction();

        builder.Services.AddDbContext<AppDbContext>(options =>
        {
            if (isProduction)
            {
                Console.WriteLine("[DEBUG] NUVEM DETECTADA: Usando Turso LibSql (Hardcoded)");

                string tursoUrl = "https://arkhos-antonieduardoso.aws-us-east-1.turso.io";
                string authToken = "eyJhbGciOiJFZERTQSIsInR5cCI6IkpXVCJ9.eyJhIjoicm8iLCJleHAiOjE3ODUwODU1NDEsImlhdCI6MTc3NzMwOTU0MSwiaWQiOiIwMTlkY2YxNi01OTAxLTc5NzQtOTI0Zi0xZThiNmZmMjhiOTciLCJyaWQiOiI1MGU0NDk4ZS04YWNhLTQ0NDUtYmU1Mi0zMjA5NmI4NDM0MTYifQ.qf6I8DV05ZP_orB7FJ1d2SYGfi22vqDRE3bkWzxiJGdukoLY9Tm76cdU31yhrq7cmgFjfq_OyQCAIauR0aFHCA";

                string connectionString = $"url={tursoUrl};jwt={authToken}";

                Console.WriteLine($"[DEBUG] String formatada: url={tursoUrl};jwt=[PROTECTED]");

                options.UseLibSql(connectionString);
            }
            else
            {
                options.UseSqlite("Data Source=../arkhos.db");
            }
        });
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