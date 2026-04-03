using System.Text.Json.Serialization;

namespace Arkhos.Core.Models.Dto.SchoolRating;
public class SchoolRatingMapDto
{
    [JsonPropertyName("school_id")]
    public int SchoolInfoId { get; set; }

    [JsonPropertyName("ano")]
    public int Ano {get;set;}
    [JsonPropertyName("rating_acessibilidade")]
    public double RatingAcessibilidade {get;set;}
    [JsonPropertyName("rating_recreacao")]
    public double RatingRecreacao {get;set;}

    [JsonPropertyName("rating_bem_estar")]
    public double RatingBemEstar {get;set;}

    [JsonPropertyName("rating_suporte_humano")]
    public double? RatingSuporteHumano {get;set;}

    [JsonPropertyName("rating_administracao")]
    public double RatingAdministracao {get;set;}

    [JsonPropertyName("distorcao_idade_serie")]
    public double? DistorcaoIdadeSerie {get;set;}

    [JsonPropertyName("rating_pedagogico")]
    public double RatingPedagogico {get;set;}

    [JsonPropertyName("rating_stress_professor")]
    public double RatingStressProfessor {get;set;}

    [JsonPropertyName("rating_instabilidade_professor")]
    public double RatingInstabilidadeProfessor {get;set;}

    [JsonPropertyName("rating_caos_administracao")]
    public double RatingCaosAdministracao {get;set;}

    [JsonPropertyName("meso_id")]
    public int MesorregiaoId {get;set;}
    [JsonPropertyName("micro_id")]
    public int MicrorregiaoId {get;set;}

    [JsonPropertyName("municipio_id")]
    public int MunicipioId {get;set;}
}