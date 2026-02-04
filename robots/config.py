import os
import time

from .portal_type1 import exec1
from .portal_type2 import exec2
from .portal_type3 import exec3
from .portal_type4 import exec4
from .portal_type5 import exec5
from .portal_type6 import exec6
from .portal_type7 import exec7
from .portal_type10 import exec10



from .driver_setup import get_driver
from .verificar_dados import verificar_dados
from .core.state import ScrapingState

"""
Água Branca	2700102
Arapiraca	2700300
Atalaia	2700409
Barra de Santo Antônio	2700508
Barra de São Miguel	2700607
Batalha	2700706
Belém	2700805
Belo Monte	2700904
Boca da Mata	2701001
Branquinha	2701100
Cacimbinhas	2701209
Cajueiro	2701308
Campestre	2701357
Campo Alegre	2701407
Campo Grande	2701506
Canapi	2701605
Capela	2701704
Carneiros	2701803
Chã Preta	2701902
Coqueiro Seco	2702207
Coruripe	2702306
Craíbas	2702355
Delmiro Gouveia	2702405
Dois Riachos	2702504
Estrela de Alagoas	2702553
Feira Grande	2702603
Feliz Deserto	2702702
Ibateguara	2703007
Igaci	2703106
Igreja Nova	2703205
Inhapi	2703304
Jacaré dos Homens	2703403
Japaratinga	2703601
Jequiá da Praia	2703759
Joaquim Gomes	2703809
Jundiá	2703908
Junqueiro	2704005
Lagoa da Canoa	2704104
Limoeiro de Anadia	2704203
Maceió	2704302
Major Isidoro	2704401
Maragogi	2704500
Maravilha	2704609
Marechal Deodoro	2704708
Maribondo	2704807
Mar Vermelho	2704906
Mata Grande	2705002
Matriz de Camaragibe	2705101
Messias	2705200
Minador do Negrão	2705309
Monteirópolis	2705408
Murici	2705507
Novo Lino	2705606
Olho d'Água das Flores	2705705
Olho d'Água do Casado	2705804
Olho d'Água Grande	2705903
Olivença	2706000
Ouro Branco	2706109
Palestina	2706208
Palmeira dos Índios	2706307
Pão de Açúcar	2706406
Pariconha	2706422
Paripueira	2706448
Passo de Camaragibe	2706505
Paulo Jacinto	2706604
Penedo	2706703
Piaçabuçu	2706802
Pilar	2706901
Pindoba	2707008
Piranhas	2707107
Poço das Trincheiras	2707206
Porto Calvo	2707305
Porto de Pedras	2707404
Porto Real do Colégio	2707503
Quebrangulo	2707602
Rio Largo	2707701
Roteiro	2707800
Santa Luzia do Norte	2707909
Santana do Ipanema	2708006
Santana do Mundaú	2708105
São Brás	2708204
São José da Laje	2708303
São José da Tapera	2708402
São Luís do Quitunde	2708501
São Miguel dos Campos	2708600
São Miguel dos Milagres	2708709
São Sebastião	2708808
Satuba	2708907
Senador Rui Palmeira	2708956
Tanque d'Arca	2709004
Taquarana	2709103
Teotônio Vilela	2709152
Traipu	2709202
União dos Palmares	2709301
Viçosa	2709400
"""

MUNICIPIOS_TIPO1 = [
    {
        "nome": "ARAPIRACA",
        "codigo_ibge": "2700300",
        "url": "https://transparencia.arapiraca.al.gov.br/despesas"
    }
]

MUNICIPIOS_TIPO2 = [
    {
        "nome": "BARRA_DE_SAO_MIGUEL",
        "codigo_ibge": "2700607",
        "url": "https://municipioonline.com.br/al/prefeitura/barradesaomiguel/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021, 2022]
    },
    {
        "nome": "BELEM",
        "codigo_ibge": "2700805",
        "url": "https://municipioonline.com.br/al/prefeitura/belem/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]
    },
    {
        "nome": "BELO_MONTE",
        "codigo_ibge": "2700904",
        "url": "https://www.municipioonline.com.br/al/prefeitura/belomonte/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]
    },
    {
        "nome": "CAMPO_ALEGRE",
        "codigo_ibge": "2701407",
        "url": "https://www.municipioonline.com.br/al/prefeitura/campoalegre/cidadao/despesa",
        "years_list": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
    },
    {
        "nome": "DOIS_RIACHOS",
        "codigo_ibge": "2702504",
        "url": "https://www.municipioonline.com.br/al/prefeitura/doisriachos/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2025]
    },
    {
        "nome": "IBATEGUARA",
        "codigo_ibge": "2703007",
        "url": "https://www.municipioonline.com.br/al/prefeitura/ibateguara/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]
    },
    {
        "nome": "TEOTONIO_VILELA",
        "codigo_ibge": "2709152",
        "url": "https://www.municipioonline.com.br/al/prefeitura/teotoniovilela/cidadao/despesa",
        "years_list": [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025] # 
    },
    {
        "nome": "SENADOR_RUI_PALMEIRA",
        "codigo_ibge": "2708956",
        "url": "https://www.municipioonline.com.br/al/prefeitura/senadorruipalmeira/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020] 
    },
    {
        "nome": "COLONIA_LEOPOLDINA",
        "codigo_ibge": "2702108",
        "url": "https://municipioonline.com.br/al/prefeitura/colonialeopoldina/cidadao/despesa",
        "years_list": [2017]  
    },
    {
        "nome": "INHAPI",
        "codigo_ibge": "2703304",
        "url": "https://municipioonline.com.br/al/prefeitura/inhapi/cidadao/despesa",
        "years_list": [2018, 2019, 2020, 2021]  
    },
    {
        "nome": "DELMIRO_GOUVEIA",
        "codigo_ibge": "2703304",
        "url": "https://municipioonline.com.br/al/prefeitura/delmirogouveia/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]  
    },
    {
        "nome": "MONTEIROPOLIS",
        "codigo_ibge": "2705408",
        "url": "https://municipioonline.com.br/al/prefeitura/monteiropolis/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]  
    },
    {
        "nome": "PIRANHAS",
        "codigo_ibge": "2707107",
        "url": "https://municipioonline.com.br/al/prefeitura/piranhas/cidadao/despesa#empenhos",
        "years_list": [2017, 2018, 2019, 2020]  
    },
    {
        "nome": "QUEBRANGULO",
        "codigo_ibge": "2707602",
        "url": "https://municipioonline.com.br/al/prefeitura/quebrangulo/cidadao/despesa",
        "years_list": [2017, 2018, 2019]  
    },
    {
        "nome": "SANTANA_DO_IPANEMA",
        "codigo_ibge": "2708006",
        "url": "https://municipioonline.com.br/al/prefeitura/santanadoipanema/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020]  
    },
    {
        "nome": "TANQUE_D_ARCA",
        "codigo_ibge": "2709004",
        "url": "https://municipioonline.com.br/al/prefeitura/tanquedarca/cidadao/despesa",
        "years_list": [2018, 2019, 2020]  
    },
    {
        "nome": "CORURIPE",
        "codigo_ibge": "2702306",
        "url": "https://municipioonline.com.br/al/prefeitura/coruripe/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020]  
    },
    {
        "nome": "AGUA_BRANCA",
        "codigo_ibge": "2700102",
        "url": "https://municipioonline.com.br/al/prefeitura/aguabranca/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020]  
    },
    {
        "nome": "AGUA_BRANCA",
        "codigo_ibge": "2704500",
        "url": "https://municipioonline.com.br/al/prefeitura/maragogi/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020]  
    },
    {
        "nome": "PAO_DE_ACUCAR",
        "codigo_ibge": "2706406",
        "url": "https://municipioonline.com.br/al/prefeitura/paodeacucar/cidadao/despesa",
        "years_list": [2019, 2020, 2022, 2023, 2024, 2025]
    },
    {
        "nome": "JEQUIA_DA_PRAIA",
        "codigo_ibge": "2703759",
        "url": "https://municipioonline.com.br/al/prefeitura/jequiadapraia/cidadao/despesa",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },
    {
        "nome": "ESTRELA_DE_ALAGOAS",
        "codigo_ibge": "2702553",
        "url": "https://municipioonline.com.br/al/prefeitura/estreladealagoas/cidadao/despesa",
        "years_list": [2017, 2018, 2019, 2020, 2021]
    },


    
]

MUNICIPIOS_TIPO3 = [
    {
        "nome": "BATALHA",
        "codigo_ibge": "2700706",
        "url": "https://portalbatalha.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024]
    },
    {
        "nome": "BRANQUINHA",
        "codigo_ibge": "2701100",
        "url": "https://portalbranquinha.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "CANAPI",
        "codigo_ibge": "2701605",
        "url": "https://portalpmcanapi.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "FLEXEIRAS",
        "codigo_ibge": "2702801",
        "url": "http://portalflexeiras.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },

    {
        "nome": "IGACI",
        "codigo_ibge": "2703106",
        "url": "http://portalflexeiras.tcgestaopublica.com.br/RelacaoEmpenho",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    },
]

MUNICIPIOS_TIPO4 = [
    {
        "nome": "BARRA_DE_SAO_MIGUEL_2",
        "codigo_ibge": "2700607",
        "url": "https://www.kalana.com.br/transparencia/12263869000108/0000/despesas?m=false",
        "api_url": "https://kalana.com.br/mobiledados"
    }
]

MUNICIPIOS_TIPO5 = [
    {

    }
]

MUNICIPIOS_TIPO6 = [
    {
        "nome": "CORURIPE",
        "codigo_ibge": "2702306",
        "url": "https://transparencia.betha.cloud/#/__zskWgzCFzFPEOT7Ihn8Q==/consulta/188066",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    }
]

MUNICIPIOS_TIPO7 = [
    {
        "nome": "JACUIPE",
        "codigo_ibge": "2703502",
        "url": "https://sistemas.jacuipe.al.gov.br:8079/transparencia/Default.aspx?AcessoIndividual=lnkDespesasPor_ProjetoAtividade",
        "years_list": [2021, 2022, 2023, 2024, 2025]
    }
]


MUNICIPIOS_TIPO10 = [
    {
        "nome": "OURO_BRANCO",
        "codigo_ibge": "2706109",
        "url": "https://admin.ourobranco.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
    {
        "nome": "CAMPESTRE",
        "codigo_ibge": "2701357",
        "url": "https://admin.campestre.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao" 
    },
    {
        "nome": "CAPELA",
        "codigo_ibge": "2701704",
        "url": "https://admin.capela.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
    {
        "nome": "CARNEIROS",
        "codigo_ibge": "2701803",
        "url": "https://admin.carneiros.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
    {
        "nome": "JAPARATINGA",
        "codigo_ibge": "2703601",
        "url": "https://admin.japaratinga.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
    {
        "nome": "MATA_GRANDE",
        "codigo_ibge": "2705002",
        "url": "https://admin.matagrande.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
    {
        "nome": "POCO_DAS_TRINCHEIRAS",
        "codigo_ibge": "2707206",
        "url": "https://admin.pocodastrincheiras.al.gov.br/api/siap-empenho?with=credor,funcao,subfuncao,undOrcamentaria,pagamentos,liquidacoes,acao"
    },
]


def exec_robots():

    base_dir = os.getcwd()

    downloads_folder = os.path.join(base_dir, "data", "raw")

    

    state_path = os.path.join(
        base_dir,
        "data",
        "state",
        "scraping_state_global.csv" 
    )

    print(state_path)

    state = ScrapingState()

    state.load_csv(state_path)
    

    time_execs = []

    try:

        # driver, wait = get_driver(downloads_folder)
        # exec1(MUNICIPIOS_TIPO1, driver, wait, downloads_folder, state)
        # driver.quit()

        # driver, wait = get_driver(downloads_folder)
        # exec2(MUNICIPIOS_TIPO2, driver, wait, downloads_folder, state)
        # driver.quit()

        # driver, wait = get_driver(downloads_folder)
        # exec3(MUNICIPIOS_TIPO3, driver, wait, downloads_folder, state)
        # driver.quit()

        # exec4(MUNICIPIOS_TIPO4, driver, wait, downloads_folder)

        # exec5()

        # driver, wait = get_driver(downloads_folder)
        # exec6(MUNICIPIOS_TIPO6, driver, wait, downloads_folder, state)
        # driver.quit()

        driver, wait = get_driver(downloads_folder)
        exec7(MUNICIPIOS_TIPO7, driver, wait, downloads_folder, state)
        driver.quit()

        # exec10(MUNICIPIOS_TIPO10, downloads_folder, state)


        state.save_csv(state_path)

        # verificar_dados()
    

    finally:
        driver.quit()
        print("Finalizado o web scrapping")

