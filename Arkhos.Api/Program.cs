using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);

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

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.MapGet("/", () => "Hello World!");

app.Run();
