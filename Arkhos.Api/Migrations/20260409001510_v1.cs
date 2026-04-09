using System;
using Microsoft.EntityFrameworkCore.Migrations;

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
                    municipio_id = table.Column<int>(type: "INTEGER", nullable: false),
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    nome_municipio = table.Column<string>(type: "TEXT", nullable: false),
                    nome_mesorregiao = table.Column<string>(type: "TEXT", nullable: false),
                    id_mesorregiao = table.Column<int>(type: "INTEGER", nullable: false),
                    nome_microrregiao = table.Column<string>(type: "TEXT", nullable: false),
                    id_microrregiao = table.Column<int>(type: "INTEGER", nullable: false),
                    area_territorial = table.Column<int>(type: "REAL", nullable: false),
                    populacao_total = table.Column<int>(type: "REAL", nullable: false),
                    densidade_demografica = table.Column<double>(type: "REAL", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_info", x => new { x.municipio_id, x.ano });
                });

            migrationBuilder.CreateTable(
                name: "city_transparency_portal",
                columns: table => new
                {
                    id = table.Column<string>(type: "TEXT", nullable: false),
                    municipio_id_fk = table.Column<int>(type: "INTEGER", nullable: false),
                    data = table.Column<DateTime>(type: "TEXT", nullable: true),
                    valor = table.Column<double>(type: "REAL", nullable: true),
                    credor = table.Column<string>(type: "TEXT", nullable: true),
                    elemento_despesa = table.Column<string>(type: "TEXT", nullable: true),
                    detalhe = table.Column<string>(type: "TEXT", nullable: true),
                    eixo = table.Column<string>(type: "TEXT", nullable: false),
                    macro = table.Column<string>(type: "TEXT", nullable: false),
                    micro = table.Column<string>(type: "TEXT", nullable: false),
                    portal_origem = table.Column<int>(type: "INTEGER", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_city_transparency_portal", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "school_infra_dict",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    variavel = table.Column<string>(type: "TEXT", nullable: false),
                    descricao = table.Column<string>(type: "TEXT", nullable: false),
                    tipo = table.Column<string>(type: "TEXT", nullable: false),
                    tamanho = table.Column<int>(type: "INTEGER", nullable: false),
                    grupo = table.Column<string>(type: "TEXT", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_infra_dict", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "SchoolEnrollDicts",
                columns: table => new
                {
                    id = table.Column<int>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    variavel = table.Column<string>(type: "TEXT", nullable: false),
                    descricao = table.Column<string>(type: "TEXT", nullable: false),
                    tipo = table.Column<string>(type: "TEXT", nullable: false),
                    tamanho = table.Column<int>(type: "INTEGER", nullable: false),
                    grupo = table.Column<string>(type: "TEXT", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SchoolEnrollDicts", x => x.id);
                });

            migrationBuilder.CreateTable(
                name: "school_info",
                columns: table => new
                {
                    escola_id = table.Column<int>(type: "INTEGER", nullable: false),
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    nome_escola = table.Column<string>(type: "TEXT", nullable: false),
                    dependencia = table.Column<short>(type: "INTEGER", nullable: false),
                    Localizacao = table.Column<short>(type: "INTEGER", nullable: true),
                    funcionamento = table.Column<short>(type: "INTEGER", nullable: false),
                    sede = table.Column<int>(type: "INTEGER", nullable: true),
                    alocacao = table.Column<short>(type: "INTEGER", nullable: false),
                    ocupacao = table.Column<short>(type: "INTEGER", nullable: false),
                    endereco = table.Column<string>(type: "TEXT", nullable: true),
                    telefone = table.Column<string>(type: "TEXT", nullable: true),
                    lat = table.Column<double>(type: "REAL", nullable: true),
                    lon = table.Column<double>(type: "REAL", nullable: true),
                    id_municipio_fk = table.Column<int>(type: "INTEGER", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_info", x => new { x.escola_id, x.ano });
                    table.ForeignKey(
                        name: "FK_school_info_city_info_id_municipio_fk_ano",
                        columns: x => new { x.id_municipio_fk, x.ano },
                        principalTable: "city_info",
                        principalColumns: new[] { "municipio_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_enroll_values",
                columns: table => new
                {
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    id_escola_fk = table.Column<int>(type: "INTEGER", nullable: false),
                    id_atributo = table.Column<int>(type: "INTEGER", nullable: false),
                    tipo_atributo = table.Column<string>(type: "TEXT", nullable: false),
                    valor = table.Column<double>(type: "REAL", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_enroll_values", x => new { x.ano, x.id_escola_fk, x.id_atributo });
                    table.ForeignKey(
                        name: "FK_school_enroll_values_SchoolEnrollDicts_id_atributo",
                        column: x => x.id_atributo,
                        principalTable: "SchoolEnrollDicts",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_school_enroll_values_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_infra_values",
                columns: table => new
                {
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    id_escola_fk = table.Column<int>(type: "INTEGER", nullable: false),
                    id_atributo = table.Column<int>(type: "INTEGER", nullable: false),
                    tipo_atributo = table.Column<string>(type: "TEXT", nullable: false),
                    valor = table.Column<double>(type: "REAL", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_infra_values", x => new { x.ano, x.id_escola_fk, x.id_atributo });
                    table.ForeignKey(
                        name: "FK_school_infra_values_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_school_infra_values_school_infra_dict_id_atributo",
                        column: x => x.id_atributo,
                        principalTable: "school_infra_dict",
                        principalColumn: "id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "school_rating",
                columns: table => new
                {
                    id_escola_fk = table.Column<int>(type: "INTEGER", nullable: false),
                    ano = table.Column<int>(type: "INTEGER", nullable: false),
                    acessibility_rating = table.Column<double>(type: "REAL", nullable: false),
                    recreation_rating = table.Column<double>(type: "REAL", nullable: false),
                    wellbeing_rating = table.Column<double>(type: "REAL", nullable: false),
                    human_support_rating = table.Column<double>(type: "REAL", nullable: true),
                    management_rating = table.Column<double>(type: "REAL", nullable: false),
                    age_grade_distortion_rating = table.Column<double>(type: "REAL", nullable: true),
                    pedagogical_rating = table.Column<double>(type: "REAL", nullable: false),
                    teacher_stress_rating = table.Column<double>(type: "REAL", nullable: false),
                    teacher_instability_rating = table.Column<double>(type: "REAL", nullable: false),
                    administrative_burden_rating = table.Column<double>(type: "REAL", nullable: false),
                    spending_per_student = table.Column<double>(type: "REAL", nullable: false),
                    spending_per_teacher = table.Column<double>(type: "REAL", nullable: false),
                    pedagogical_spending_per_student = table.Column<double>(type: "REAL", nullable: false),
                    infrastructure_spending_per_student = table.Column<double>(type: "REAL", nullable: false),
                    meal_spending_per_student = table.Column<double>(type: "REAL", nullable: false),
                    transport_spending_per_student = table.Column<double>(type: "REAL", nullable: false),
                    approval_rate = table.Column<double>(type: "REAL", nullable: true),
                    failure_rate = table.Column<double>(type: "REAL", nullable: true),
                    dropout_rate = table.Column<double>(type: "REAL", nullable: true),
                    ideb_rating = table.Column<double>(type: "REAL", nullable: true),
                    saeb_rating = table.Column<double>(type: "REAL", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_school_rating", x => new { x.id_escola_fk, x.ano });
                    table.ForeignKey(
                        name: "FK_school_rating_school_info_id_escola_fk_ano",
                        columns: x => new { x.id_escola_fk, x.ano },
                        principalTable: "school_info",
                        principalColumns: new[] { "escola_id", "ano" },
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_atributo",
                table: "school_enroll_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_enroll_values_id_escola_fk_ano",
                table: "school_enroll_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_info_id_municipio_fk_ano",
                table: "school_info",
                columns: new[] { "id_municipio_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_atributo",
                table: "school_infra_values",
                column: "id_atributo");

            migrationBuilder.CreateIndex(
                name: "IX_school_infra_values_id_escola_fk_ano",
                table: "school_infra_values",
                columns: new[] { "id_escola_fk", "ano" });

            migrationBuilder.CreateIndex(
                name: "IX_school_rating_ano",
                table: "school_rating",
                column: "ano");
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "city_transparency_portal");

            migrationBuilder.DropTable(
                name: "school_enroll_values");

            migrationBuilder.DropTable(
                name: "school_infra_values");

            migrationBuilder.DropTable(
                name: "school_rating");

            migrationBuilder.DropTable(
                name: "SchoolEnrollDicts");

            migrationBuilder.DropTable(
                name: "school_infra_dict");

            migrationBuilder.DropTable(
                name: "school_info");

            migrationBuilder.DropTable(
                name: "city_info");
        }
    }
}
