namespace Arkhos.Core.Models;
public class CityTransparencyPortal
{
    public string Id { get; set; } = null!;
    public int MuncipioId { get; set; }
    public CityInfo CityInfo { get; set; } = null!;
    public DateTime? Data { get; set; }
    public double? Valor { get; set; }
    public string? Credor  { get; set; }
    public string? ElementoDespesa { get; set; }
    public string? Detalhe { get; set; }
    public string Eixo { get; set; } = string.Empty;
    public string Macro { get; set; } = string.Empty;
    public string Micro { get; set; } = string.Empty;
    public int PortalOrigem { get; set; }
}