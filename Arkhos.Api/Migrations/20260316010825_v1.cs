using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

namespace Arkhos.Api.Migrations
{
    /// <inheritdoc />
    public partial class v1 : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "city_info",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    nome_municipio = table.Column<string>(type: "text", nullable: false),
                    nome_mesorregiao = table.Column<string>(type: "text", nullable: false),
                    id_microrregiao = table.Column<long>(type: "bigint", nullable: false),
                    nome_microrregiao = table.Column<string>(type: "text", nullable: false),
                    IdMicrorregiao = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_info", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "school_info",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    nome_escola = table.Column<string>(type: "text", nullable: false),
                    depedencia = table.Column<short>(type: "smallint", nullable: false),
                    Localizacao = table.Column<short>(type: "smallint", nullable: false),
                    funcionamento = table.Column<short>(type: "smallint", nullable: false),
                    sede = table.Column<int>(type: "int", nullable: true),
                    alocacao = table.Column<short>(type: "smallint", nullable: false),
                    ocupacao = table.Column<short>(type: "smallint", nullable: false),
                    ano = table.Column<int>(type: "int", nullable: false),
                    endereco = table.Column<string>(type: "text", nullable: false),
                    telefone = table.Column<string>(type: "text", nullable: false),
                    id_municipio_fk = table.Column<long>(type: "bigint", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_info", x => x.Id);
                    table.ForeignKey(
                        name: "FK_school_info_city_info_id_municipio_fk",
                        column: x => x.id_municipio_fk,
                        principalTable: "city_info",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_rating",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:IdentitySequenceOptions", "'1', '1', '', '', 'False', '1'")
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityByDefaultColumn),
                    id_school_fk = table.Column<long>(type: "bigint", nullable: false),
                    ano = table.Column<int>(type: "int", nullable: false),
                    acessibility_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    recreation_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    wellbeing_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    human_support_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    management_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    age_grade_distortion_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    pedagogical_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    teacher_stress_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    teacher_instability_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    administrative_burden_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    spending_per_teacher = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    pedagogical_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    infrastructure_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    meal_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    transport_spending_per_student = table.Column<double>(type: "numeric(14,4)", nullable: false),
                    approval_rate = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    failure_rate = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    dropout_rate = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    ideb_rating = table.Column<double>(type: "numeric(5,4)", nullable: false),
                    saeb_rating = table.Column<double>(type: "numeric(5,4)", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_rating", x => x.Id);
                    table.ForeignKey(
                        name: "FK_school_rating_school_info_id_school_fk",
                        column: x => x.id_school_fk,
                        principalTable: "school_info",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_school_info_id_municipio_fk",
                table: "school_info",
                column: "id_municipio_fk");

            migrationBuilder.CreateIndex(
                name: "IX_school_rating_id_school_fk",
                table: "school_rating",
                column: "id_school_fk",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "school_rating");

            migrationBuilder.DropTable(
                name: "school_info");

            migrationBuilder.DropTable(
                name: "city_info");
        }
    }
}
