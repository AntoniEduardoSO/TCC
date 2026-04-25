using System.Xml.Linq;
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
        
        builder.Property(x => x.Id)
               .HasColumnName("id")
               .ValueGeneratedOnAdd(); 

        builder.Property(x => x.Axis)
               .HasColumnName("axis")
               .HasColumnType("TEXT");

        builder.Property(x => x.Level)
               .HasColumnName("level")
               .HasColumnType("TEXT");

        builder.Property(x => x.Ano)
               .HasColumnName("ano")
               .HasColumnType("INTEGER");

        builder.Property(x => x.TipoInsight)
               .HasColumnName("tipo_insight")
               .HasColumnType("TEXT");

        builder.Property(x => x.Titulo)
               .HasColumnName("titulo")
               .HasColumnType("TEXT");

        builder.Property(x => x.ValorDestaque)
               .HasColumnName("valor_destaque")
               .HasColumnType("TEXT");

        builder.Property(x => x.Descricao)
               .HasColumnName("descricao")
               .HasColumnType("TEXT");

        builder.Property(x => x.Recomendacao)
               .HasColumnName("recomendacao")
               .HasColumnType("TEXT");

        builder.Property(x => x.ValorBaseline)
               .HasColumnName("valor_baseline")
               .HasColumnType("REAL");

        builder.Property(x => x.IdAlvo)
               .HasColumnName("id_alvo")
               .HasColumnType("INTEGER");
    }
}