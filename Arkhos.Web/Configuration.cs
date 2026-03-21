using MudBlazor;
using MudBlazor.Utilities;

namespace Arkhos.Web;
public static class Configuration
{
    public const string HttpClientName = "arkhos";

    public static string BackendUrl { get; set; } = "http://localhost:5040";

}