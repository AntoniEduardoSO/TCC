using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolEnrollValues;
public class SchoolEnrollmentDetailDto 
{
    [JsonPropertyName("escola_id")]
    public int EscolaId { get; set; }
    
    [JsonPropertyName("nome_escola")]
    public string NomeEscola { get; set; } = string.Empty;

    [JsonPropertyName("ano")]
    public int Ano { get; set; }

    // Matriculas.

    [JsonPropertyName("matricula_creche")]
    public double MatriculaCreche { get; set; }
    [JsonPropertyName("matricula_pre_escola")]
    public double MatriculaPreEscola { get; set; }

    [JsonPropertyName("matricula_ensino_fundamental_ai")]
    public double MatriculaEnsinoFundamentalAI { get; set; }
    
    [JsonPropertyName("matricula_ensino_fundamental_af")]
    public double MatriculaEnsinoFundamentalAF { get; set; }
    
    [JsonPropertyName("matricula_ensino_medio")]
    public double MatriculaEnsinoMedio { get; set; }

    // Corpo Docente.
    
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

    // Ajuda pedagogica.

    [JsonPropertyName("psicologo")]
    public double Psicologo {get;set;}
    [JsonPropertyName("fonaudiologo")]
    public double Fonaudiologo {get;set;}
    [JsonPropertyName("assistente_social")]
    public double AssistenteSocial {get;set;}
    [JsonPropertyName("tradutor_libras")]
    public double TradutorLibras {get;set;}
    [JsonPropertyName("associacao_pais_e_mestres")]
    public double AssociacaoPaiMestres {get;set;}
    [JsonPropertyName("conselho_escolar")]
    public double ConselhoEscolar {get;set;}
    [JsonPropertyName("gremio_estudantil")]
    public double GremioEstudantil {get;set;}
}