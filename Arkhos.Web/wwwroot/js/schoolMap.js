
let map;
let markerCluster;
let allMarkersData = [];
let levelStack = [];

let currentLevel = "meso"; // padrão inicial
let geoLayerGroup;

let currentFilterFn = null;
let currentMarkerFilterFn = null;

let blazorRef = null; 
let currentClickedLevel = null;
let currentClickedId = null;

const geoJsonUrls = {
    meso: "mesorregioes.json",
    micro: "microrregioes.json",
    municipio: "municipios.json"
};

function getStyle(level) {
    // Deixar por enquanto o style assim, depois corrigir
    switch (level) {
        case "meso":
            return { color: "red", weight: 2, fillOpacity: 0.2 };

        case "micro":
            return { color: "green", weight: 1.5, fillOpacity: 0.2 };

        case "municipio":
            return { color: "blue", weight: 1, fillOpacity: 0.1 };
    }

};

function handleFeatureClick(feature, level)
{
    const rawId = feature.properties.id;
    const numericId = rawId ? Number(rawId) : null;

    if (level === "meso") {

        // Se clicar no meso, devolvemos todos os seus micros.
        navigateTo(
            "micro",
            (f) => f.properties.meso_id === rawId,
            (m) => m.meso_id === numericId,
            "meso",
            numericId
        );
    }

    else if (level === "micro") {

        // Se clicar no micro, devolvemos todos os seus micros.
        navigateTo(
            "municipio",
            (f) => Number(f.properties.microrregiao?.micro_id) === numericId,
            (m) => Number(m.micro_id) === numericId,
            "micro",
            numericId
        );
    }

    else if (level === "municipio") {
        navigateTo(
            "municipio",
            (f) => Number(f.properties.id) === numericId,
            (m) => Number(m.municipio_id) === numericId,
            "municipio",
            numericId
        );
    }
};

function navigateTo(level, filterFn, markerFilterFn, clickedLevel, clickedId) {

    // salva estado atual antes de mudar
    levelStack.push({
        level: currentLevel,
        filter: currentFilterFn,
        markerFilter: currentMarkerFilterFn,
        clickedLevel: currentClickedLevel,
        clickedId: currentClickedId
    });

    // atualiza estado atual
    currentLevel = level;
    currentFilterFn = filterFn;
    currentMarkerFilterFn = markerFilterFn;
    currentClickedLevel = clickedLevel;
    currentClickedId = clickedId;

    // aplica
    loadGeoLayer(level, filterFn);

    if (markerFilterFn) {
        renderMarkers(allMarkersData.filter(markerFilterFn));
    } else {
        renderMarkers(allMarkersData);
    }

    if (blazorRef) {
        blazorRef.invokeMethodAsync('ApplyFilter', currentClickedLevel, currentClickedId);
    }
}

window.goBack = () => {

    if (levelStack.length === 0) {
        console.log("Já está no nível inicial");
        return;
    }

    const previous = levelStack.pop();

    if (!previous) return;

    currentLevel = previous.level;
    currentFilterFn = previous.filter;
    currentMarkerFilterFn = previous.markerFilter;
    currentClickedLevel = previous.clickedLevel;
    currentClickedId = previous.clickedId;

    loadGeoLayer(currentLevel, currentFilterFn);

    if (currentMarkerFilterFn) {
        renderMarkers(allMarkersData.filter(currentMarkerFilterFn));
    } else {
        renderMarkers(allMarkersData);
    }

    if (blazorRef) {
        blazorRef.invokeMethodAsync('ApplyFilter', currentClickedLevel, currentClickedId);
    }
};

async function loadGeoLayer(level, filterFn = null) {
    currentLevel = level;


    // Remove o layer antigo.
    geoLayerGroup.clearLayers();

    // Pega o json baseado no level.
    const url = geoJsonUrls[level];

    // Faz a requisicao correta.
    const response = await fetch(url);
    let geojson = await response.json();

    // Filtrar pela variavel escolhida meso 1-> mostra as micro dela.

    if (filterFn) {
        geojson.features = geojson.features.filter(filterFn);
    }

    // Cria o gejson correto.
    const layer = L.geoJSON(geojson, {

        style: getStyle(level),

        onEachFeature: (feature, layer) => {

            const nome = feature.properties.nome || feature.properties.name;

            layer.bindPopup(`<b>${nome}</b>`);

            // clique muda o level
            layer.on("click", () => handleFeatureClick(feature, level));
        }

    }).addTo(map);


    geoLayerGroup.addLayer(layer);

    // Ajusta o zoom do mapa.
    map.fitBounds(layer.getBounds());
};

window.initMap = async(geoJsonUrl, dotNetRef) =>
{ 
    blazorRef = dotNetRef;

    // Se tiver ja um mapa, apagamos o anterior, e criamos um novo (posteriormente com os filtros, isso vai ser necessario.)
    if(map){
        map.remove()
    }

    // Criar o mapa no local medio de alagoas.
    map = L.map('map').setView([-9.66625, -35.7351], 8);

    // tile por enquanto openstreetmap, depois criar uma funcao que pega se o usuario usar dark ou light theme
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
        attribution: '© OpenStreetMap'
    }).addTo(map);

    geoLayerGroup = L.layerGroup().addTo(map);

    // Criar os clusters vazios, pois vao ser populados na outra funcao.
    markerCluster = L.markerClusterGroup();
    map.addLayer(markerCluster);

    // Carregar o level inicial
    await loadGeoLayer("meso");

};

// Renderizar os markers.
function renderMarkers(data) {

    markerCluster.clearLayers();

    data.forEach(item => {
        if (!item.lat || !item.lon) return;

        const marker = L.marker([item.lat, item.lon]);

        marker.bindPopup(`
            <b>${item.nomeEscola}</b><br/>
            ${item.endereco ?? ""}
        `);

        markerCluster.addLayer(marker);
    });
}


// Fazer update dos markers (fazer isso para evitar sobrecarga)
window.updateMarkers = (data) => {
    allMarkersData = data;
    renderMarkers(data);

};