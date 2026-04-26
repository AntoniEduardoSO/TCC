namespace Arkhos.Core.Requests.SchoolEnrollValues;
public class GetRegionEnrollmentSummaryByFilterRequest : Request
{
    public int Year { get; set; }
    public int? Depedencia {get;set;}

    public int? MesorregiaoId { get; set; }
    public int? MicrorregiaoId { get; set; }
    public int? MunicipioId { get; set; }
}