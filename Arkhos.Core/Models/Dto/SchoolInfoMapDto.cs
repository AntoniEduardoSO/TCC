namespace Arkhos.Core.Models.Dto;
public class SchoolInfoMapDto
{
    public int Id { get; set; }
    public int IdEscola { get; set; }
    public string NomeEscola { get; set; } = string.Empty;
    public string Endereco { get; set; } = string.Empty;
    public string Telefone { get; set; } = string.Empty;
    public int Ano { get; set; }

    public double? Lat {get;set;}
    public double? Lon {get;set;}
}