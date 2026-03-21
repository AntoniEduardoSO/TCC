using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Arkhos.Web;
using MudBlazor.Services;
using Arkhos.Core.Handlers;
using Arkhos.Web.Handlers;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

builder.Services.AddMudServices();

Configuration.BackendUrl = builder.Configuration.GetValue<string>("BackendUrl") ?? string.Empty;

builder.Services.AddHttpClient(Configuration.HttpClientName, options =>
{
    Console.WriteLine(Configuration.BackendUrl);
    options.BaseAddress = new Uri(Configuration.BackendUrl);
});

builder.Services.AddTransient<ISchoolInfosHandler, SchoolInfoHandler>();


await builder.Build().RunAsync();
