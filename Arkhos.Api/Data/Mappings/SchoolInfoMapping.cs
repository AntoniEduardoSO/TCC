using Arkhos.Core.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Arkhos.Api.Data.Mappings;

public class SchoolInfoMapping : IEntityTypeConfiguration<SchoolInfo>
{
    public void Configure(EntityTypeBuilder<SchoolInfo> builder)
    {
        builder.ToTable("school_info");

        builder.HasKey(x => x.Id);


        builder.Property(x => x.NomeEscola)
        .HasColumnType("text")
        .HasColumnName("nome_escola")
        .IsRequired();

        builder.Property(x => x.CityInfoId)
        .HasColumnName("id_municipio_fk");

        builder.Property(x => x.Depedencia)
        .HasColumnType("smallint")
        .HasColumnName("depedencia")
        .IsRequired();


        builder.Property(x => x.Funcionamento)
        .HasColumnType("smallint")
        .HasColumnName("funcionamento")
        .IsRequired();

        builder.Property(x => x.Sede)
        .HasColumnType("int")
        .HasColumnName("sede")
        .IsRequired(false);

        builder.Property(x => x.Alocacao)
        .HasColumnType("smallint")
        .HasColumnName("alocacao")
        .IsRequired();

        builder.Property(x => x.Ocupacao)
        .HasColumnType("smallint")
        .HasColumnName("ocupacao")
        .IsRequired();

        builder.Property(x => x.Ano)
        .HasColumnType("int")
        .HasColumnName("ano")
        .IsRequired();

        builder.Property(x => x.Endereco)
        .HasColumnType("text")
        .HasColumnName("endereco")
        .IsRequired();

        builder.Property(x => x.Telefone)
        .HasColumnType("text")
        .HasColumnName("telefone")
        .IsRequired();

        builder.HasOne(x => x.SchoolRating)
        .WithOne(x => x.SchoolInfo)
        .HasForeignKey<SchoolRating>(x => x.SchoolInfoId)
        .IsRequired();
    }
}