using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class CityTransparencyPortalMapping : IEntityTypeConfiguration<CityTransparencyPortal>
{
    public void Configure(EntityTypeBuilder<CityTransparencyPortal> builder)
    {
        builder.ToTable("city_transparency_portal");

        builder.HasKey(x => x.Id);

        builder.Property(x => x.Id)
            .HasColumnName("id")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.MuncipioId)
            .HasColumnName("municipio_id_fk")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Property(x => x.Data)
            .HasColumnName("data")
            .HasColumnType("TEXT"); 

        builder.Property(x => x.Valor)
            .HasColumnName("valor")
            .HasColumnType("REAL");

        builder.Property(x => x.Credor)
            .HasColumnName("credor")
            .HasColumnType("TEXT");

        builder.Property(x => x.ElementoDespesa)
            .HasColumnName("elemento_despesa")
            .HasColumnType("TEXT");

        builder.Property(x => x.Detalhe)
            .HasColumnName("detalhe")
            .HasColumnType("TEXT");

        builder.Property(x => x.Eixo)
            .HasColumnName("eixo")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Macro)
            .HasColumnName("macro")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.Micro)
            .HasColumnName("micro")
            .HasColumnType("TEXT")
            .IsRequired();

        builder.Property(x => x.PortalOrigem)
            .HasColumnName("portal_origem")
            .HasColumnType("INTEGER")
            .IsRequired();

        builder.Ignore(x => x.CityInfo);
    }
}