using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolEnrollValues;
public class SchoolEnrollValuesTeachersDto
{
    [JsonPropertyName("ano")]
    public int Ano { get; set; }
    [JsonPropertyName("escola_id")]
    public int EscolaId {get;set;}

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}

    [JsonPropertyName("professor_creche")]
    public double ProfessorCreche { get; set; }
    [JsonPropertyName("professor_pre_escola")]
    public double ProfessorPreEscola { get; set; }
    [JsonPropertyName("professor_ef_iniciais")]
    public double ProfessorEFIniciais { get; set; }
    [JsonPropertyName("professor_ef_finais")]
    public double ProfessorEFFinais { get; set; }
    [JsonPropertyName("professor_medio")]
    public double ProfessorMedio { get; set; }
    [JsonPropertyName("professor_especial")]
    public double ProfessorEspecial { get; set; }
}