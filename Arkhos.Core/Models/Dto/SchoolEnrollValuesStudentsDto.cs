using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto;
public class SchoolEnrollValuesStudentsDto
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
    [JsonPropertyName("matricula_creche")]
    public double MatriculaCreche { get; set; }
    [JsonPropertyName("matricula_pre_escola")]
    public double MatriculaPreEscola { get; set; }

    [JsonPropertyName("matricula_1_ano")]
    public double Matricula1Ano { get; set; }
    
    [JsonPropertyName("matricula_2_ano")]
    public double Matricula2Ano { get; set; }
    
    [JsonPropertyName("matricula_3_ano")]
    public double Matricula3Ano { get; set; }
    
    [JsonPropertyName("matricula_4_ano")]
    public double Matricula4Ano { get; set; }
    
    [JsonPropertyName("matricula_5_ano")]
    public double Matricula5Ano { get; set; }

    [JsonPropertyName("matricula_6_ano")]
    public double Matricula6Ano { get; set; }
    
    [JsonPropertyName("matricula_7_ano")]
    public double Matricula7Ano { get; set; }
    
    [JsonPropertyName("matricula_8_ano")]
    public double Matricula8Ano { get; set; }
    
    [JsonPropertyName("matricula_9_ano")]
    public double Matricula9Ano { get; set; }

    [JsonPropertyName("matricula_medio_1_ano")]
    public double MatriculaMedio1Ano { get; set; }
    
    [JsonPropertyName("matricula_medio_2_ano")]
    public double MatriculaMedio2Ano { get; set; }
    
    [JsonPropertyName("matricula_medio_3_ano")]
    public double MatriculaMedio3Ano { get; set; }
}

