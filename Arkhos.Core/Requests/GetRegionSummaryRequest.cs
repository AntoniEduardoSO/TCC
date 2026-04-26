namespace Arkhos.Core.Requests;

public class GetRegionSummaryRequest : Request
{
    public int Year { get; set; } = 2024;
    public int? MesorregiaoId { get; set; }
    public int? MicrorregiaoId { get; set; }
    public int? MunicipioId { get; set; }
    public int? SchoolId { get; set; }
    public int? Dependencia { get; set; }
}
