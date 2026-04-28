using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolInfraValuesMapping : IEntityTypeConfiguration<SchoolInfraValues>
{
    public void Configure(EntityTypeBuilder<SchoolInfraValues> builder)
    {
        builder.ToTable("school_infra_values");

        builder.HasKey(x => new { x.Ano, x.IdEscolaInfraValues, x.AtributoId });

        builder.HasIndex(x => x.IdEscolaInfraValues);
        builder.HasIndex(x => x.AtributoId);

        builder.Property(x => x.IdEscolaInfraValues).HasColumnName("id_escola_fk").IsRequired();
        builder.Property(x => x.Ano).HasColumnName("ano").IsRequired();
        builder.Property(x => x.AtributoId).HasColumnName("id_atributo").IsRequired();
        builder.Property(x => x.TipoAtributo).HasColumnName("tipo_atributo").IsRequired();
        builder.Property(x => x.Valor).HasColumnName("valor").IsRequired();

        builder.HasOne(x => x.SchoolInfraDict)
            .WithMany(x => x.SchoolInfraValues)
            .HasForeignKey(x => x.AtributoId)
            .HasPrincipalKey(x => x.Id);

        builder.HasOne(x => x.SchoolInfo)
            .WithMany(x => x.SchoolInfraValues)
            .HasForeignKey(x => new { x.IdEscolaInfraValues, x.Ano })
            .HasPrincipalKey(x => new { x.IdEscola, x.Ano });
    }
}