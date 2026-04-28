using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class TargetInsightMapping : IEntityTypeConfiguration<TargetInsight>
{
    public void Configure(EntityTypeBuilder<TargetInsight> builder)
    {
        builder.ToTable("insights");

        builder.HasKey(x => x.Id);
        
        builder.HasIndex(x => x.IdAlvo);
        builder.HasIndex(x => x.Ano);

        builder.Property(x => x.Id).HasColumnName("id").ValueGeneratedOnAdd(); 
        builder.Property(x => x.Axis).HasColumnName("axis");
        builder.Property(x => x.Level).HasColumnName("level");
        builder.Property(x => x.Ano).HasColumnName("ano");
        builder.Property(x => x.TipoInsight).HasColumnName("tipo_insight");
        builder.Property(x => x.Titulo).HasColumnName("titulo");
        builder.Property(x => x.ValorDestaque).HasColumnName("valor_destaque");
        builder.Property(x => x.Descricao).HasColumnName("descricao");
        builder.Property(x => x.Recomendacao).HasColumnName("recomendacao");
        builder.Property(x => x.ValorBaseline).HasColumnName("valor_baseline");
        builder.Property(x => x.IdAlvo).HasColumnName("id_alvo");
    }
}