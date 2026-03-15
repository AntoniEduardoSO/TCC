using Microsoft.OpenApi;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(x =>
{
    x.SwaggerDoc("v1", new OpenApiInfo
    {
        Version = "v1",
        Title = "ToDo Api Arkhos.",
        Description = "Um sistema de dashboard focado em gestão educacional utilizando ML preditivo-prescritivo.",
        Contact = new OpenApiContact
        {
            Name = "Github",
            Url = new Uri("https://github.com/AntoniEduardoSO/TCC")
        } 
    });

    x.CustomSchemaIds(n => n.FullName);
});


var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();


app.MapGet("/", () => "Hello World!");

app.MapGet("/v1/teste", () => new { message = "fala tu"});

app.Run();
