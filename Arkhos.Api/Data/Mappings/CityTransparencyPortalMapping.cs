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

        builder.HasIndex(x => x.MuncipioId);

        builder.Property(x => x.Id).HasColumnName("id").IsRequired();
        builder.Property(x => x.MuncipioId).HasColumnName("municipio_id_fk").IsRequired();
        builder.Property(x => x.Data).HasColumnName("data");
        builder.Property(x => x.Valor).HasColumnName("valor");
        builder.Property(x => x.Credor).HasColumnName("credor");
        builder.Property(x => x.ElementoDespesa).HasColumnName("elemento_despesa");
        builder.Property(x => x.Detalhe).HasColumnName("detalhe");
        builder.Property(x => x.Eixo).HasColumnName("eixo").IsRequired();
        builder.Property(x => x.Macro).HasColumnName("macro").IsRequired();
        builder.Property(x => x.Micro).HasColumnName("micro").IsRequired();
        builder.Property(x => x.PortalOrigem).HasColumnName("portal_origem").IsRequired();

        builder.Ignore(x => x.CityInfo);
    }
}