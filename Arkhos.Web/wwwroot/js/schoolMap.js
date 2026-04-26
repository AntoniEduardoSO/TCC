
let map;
let markerCluster;
let allMarkersData = [];
let levelStack = [];

let currentLevel = "meso"; // padrão inicial
let currentViewName = "Estado de Alagoas";
let geoLayerGroup;

let currentFilterFn = null;
let currentMarkerFilterFn = null;

let blazorRef = null; 
let currentClickedLevel = null;
let currentClickedId = null;

let infoCardControl;

const geoJsonUrls = {
    meso: "mesorregioes.json",
    micro: "microrregioes.json",
    municipio: "municipios.json"
};

function getStyle(level) {
    // Deixar por enquanto o style assim, depois corrigir
    switch (level) {
        case "meso":
            return { color: "#e2e8f0", weight: 1.5, fillOpacity: 0.04, fillColor: "#f8fafc" };

        case "micro":
            return { color: "#cbd5e1", weight: 1.2, fillOpacity: 0.04, fillColor: "#f1f5f9" };

        case "municipio":
            return { color: "#94a3b8", weight: 0.8, fillOpacity: 0.04, fillColor: "#e2e8f0" };
    }

};

function handleFeatureClick(feature, level)
{
    const rawId = feature.properties.id;
    const numericId = rawId ? Number(rawId) : null;
    const nome = feature.properties.nome || feature.properties.name;

    if (level === "meso") {
        navigateTo("micro", (f) => f.properties.meso_id === rawId, (m) => m.meso_id === numericId, "meso", numericId, `Mesorregião: ${nome}`);
    }
    else if (level === "micro") {
        navigateTo("municipio", (f) => Number(f.properties.microrregiao?.micro_id) === numericId, (m) => Number(m.micro_id) === numericId, "micro", numericId, `Microrregião: ${nome}`);
    }
    else if (level === "municipio") {
        navigateTo("municipio", (f) => Number(f.properties.id) === numericId, (m) => Number(m.municipio_id) === numericId, "municipio", numericId, `Município: ${nome}`);
    }
};

function navigateTo(level, filterFn, markerFilterFn, clickedLevel, clickedId, viewName) {

    // salva estado atual antes de mudar
    levelStack.push({
        level: currentLevel,
        filter: currentFilterFn,
        markerFilter: currentMarkerFilterFn,
        clickedLevel: currentClickedLevel,
        clickedId: currentClickedId,
        viewName: currentViewName
    });

    // atualiza estado atual
    currentLevel = level;
    currentFilterFn = filterFn;
    currentMarkerFilterFn = markerFilterFn;
    currentClickedLevel = clickedLevel;
    currentClickedId = clickedId;
    if (viewName) currentViewName = viewName;

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
    if (infoCardControl) infoCardControl.update();
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
    currentViewName = previous.viewName;

    loadGeoLayer(currentLevel, currentFilterFn);

    if (currentMarkerFilterFn) {
        renderMarkers(allMarkersData.filter(currentMarkerFilterFn));
    } else {
        renderMarkers(allMarkersData);
    }

    if (blazorRef) {
        blazorRef.invokeMethodAsync('ApplyFilter', currentClickedLevel, currentClickedId);
    }

    if (infoCardControl) infoCardControl.update();
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

            layer.bindTooltip(`<b>${nome}</b>`, {
                sticky: true,
                className: 'custom-dark-tooltip' 
            });

            layer.on("mouseover", function (e) {
                const targetLayer = e.target;
                
                targetLayer.setStyle({
                    fillOpacity: 0.25,
                    color: "#ffffff",
                    weight: 3 
                });
                
                if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
                    targetLayer.bringToFront();
                }

                // ATUALIZA O CARD COM OS DADOS DA REGIÃO
                infoCardControl.update(feature.properties);
            });

            layer.on("mouseout", function (e) {
                const targetLayer = e.target;
                targetLayer.setStyle(getStyle(level));
                
                // RETORNA O CARD PARA O ESTADO INICIAL
                infoCardControl.update();
            });

            // clique muda o level
            layer.on("click", () => handleFeatureClick(feature, level));
        }

    }).addTo(map);


    geoLayerGroup.addLayer(layer);

    // Ajusta o zoom do mapa.
    map.fitBounds(layer.getBounds(), {
        paddingTopLeft: [240, 20] // 240px de margem na esquerda
    });
};

window.initMap = async(geoJsonUrl, dotNetRef) =>
{ 
    blazorRef = dotNetRef;

    // Se tiver ja um mapa, apagamos o anterior, e criamos um novo (posteriormente com os filtros, isso vai ser necessario.)
    if(map){
        map.remove()
    }

    // Criar o mapa no local medio de alagoas.
    map = L.map('map', {
        zoomControl: false 
    }).setView([-9.66625, -35.7351], 8);

    // TileLayer Dark Matter da CartoDB!
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 20
    }).addTo(map);

    geoLayerGroup = L.layerGroup().addTo(map);

    // Criar os clusters vazios, pois vao ser populados na outra funcao.
    markerCluster = L.markerClusterGroup({
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false, // Tira aquele polígono de cobertura que é feio
        zoomToBoundsOnClick: true
    });
    map.addLayer(markerCluster);

    // CRIA O CARD DE INFORMAÇÕES
    createInfoCard();

    // Carregar o level inicial
    await loadGeoLayer("meso");

};

function createInfoCard() {
    infoCardControl = L.control({ position: 'topleft' });

    infoCardControl.onAdd = function (map) {
        this._div = L.DomUtil.create('div', 'custom-map-info-card');
        this.update(); // Atualiza com o estado inicial
        return this._div;
    };

    infoCardControl.update = function (props) {
        const icon = (path, color = "#94a3b8") => `
            <svg viewBox="0 0 24 24" fill="${color}" width="16" height="16" style="min-width: 16px; margin-right: 6px;">
                <path d="${path}"></path>
            </svg>
        `;

        const paths = {
            total: "M12 3 1 9l4 2.18v6L12 21l7-3.82v-6l2.12-1.15V17h2V9L12 3zm6.82 6L12 12.72 5.18 9 12 5.28 18.82 9z", 
            urbana: "M15 11V5l-3-3-3 3v2H3v14h18V11h-3zm-8 8H5v-2h2v2zm0-4H5v-2h2v2zm0-4H5V9h2v2zm6 8h-2v-2h2v2zm0-4h-2v-2h2v2zm0-4h-2V9h2v2zm0-4h-2V5h2v2zm6 12h-2v-2h2v2zm0-4h-2v-2h2v2z", 
            rural: "m14 6-3.75 5 2.85 3.8-1.6 1.2C9.81 13.75 7 10 7 10l-6 8h22L14 6z", 
            creche: "M22.94 12.66c.04-.21.06-.43.06-.66s-.02-.45-.06-.66c-.25-1.51-1.36-2.74-2.81-3.17-.53-1.12-1.28-2.1-2.19-2.91C16.36 3.85 14.28 3 12 3s-4.36.85-5.94 2.26c-.92.81-1.67 1.8-2.19 2.91-1.45.43-2.56 1.65-2.81 3.17-.04.21-.06.43-.06.66s.02.45.06.66c.25 1.51 1.36 2.74 2.81 3.17.52 1.11 1.27 2.09 2.17 2.89C7.62 20.14 9.71 21 12 21s4.38-.86 5.97-2.28c.9-.8 1.65-1.79 2.17-2.89 1.44-.43 2.55-1.65 2.8-3.17zM19 14c-.1 0-.19-.02-.29-.03-.2.67-.49 1.29-.86 1.86C16.6 17.74 14.45 19 12 19s-4.6-1.26-5.85-3.17c-.37-.57-.66-1.19-.86-1.86-.1.01-.19.03-.29.03-1.1 0-2-.9-2-2s.9-2 2-2c.1 0 .19.02.29.03.2-.67.49-1.29.86-1.86C7.4 6.26 9.55 5 12 5s4.6 1.26 5.85 3.17c.37.57.66 1.19.86 1.86.1-.01.19-.03.29-.03 1.1 0 2 .9 2 2s-.9 2-2 2z", 
            fund: "M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z", 
            medio: "M5 13.18v4L12 21l7-3.82v-4L12 17l-7-3.82zM12 3 1 9l11 6 9-4.91V17h2V9L12 3z" 
        };

        let headerHtml = "";

        if (props) {
            // ESTADO HOVER: Quando o mouse está por cima de uma região
            const nome = props.nome || props.name;
            let labelContexto = "Região";
            if (currentLevel === "meso") labelContexto = "Mesorregião";
            else if (currentLevel === "micro") labelContexto = "Microrregião";
            else if (currentLevel === "municipio") labelContexto = "Município";

            headerHtml = `
                <span style="font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px;">${labelContexto}</span><br/>
                <h4 style="color: #38bdf8; margin: 4px 0 12px 0; font-size: 16px;">${nome}</h4>
            `;
        } else {
            // ESTADO REPOUSO: Quando o mouse sai, exibe a região global clicada
            headerHtml = `
                <span style="font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px;">Visão Geral</span><br/>
                <h4 style="color: #e2e8f0; margin: 4px 0 12px 0; font-size: 16px;">${currentViewName}</h4>
            `;
        }

        // SIMULADOR DE DADOS: 
        // Se estivermos na visão macro inicial (Estado), mostramos um volume grande de escolas (ex: 1450).
        // Se for hover ou tiver filtrado, exibe os valores menores proporcionais.
        const isAlagoasBase = !props && currentViewName === "Estado de Alagoas";
        
        const total = isAlagoasBase ? 1450 : (Math.floor(Math.random() * 150) + 20); 
        const urbanas = isAlagoasBase ? 840 : Math.floor(total * 0.6);
        const rurais = total - urbanas;
        
        const creche = isAlagoasBase ? 350 : Math.floor(total * 0.2);
        const fund = isAlagoasBase ? 890 : Math.floor(total * 0.6);
        const medio = total - creche - fund;

        this._div.innerHTML = `
            ${headerHtml}
            
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.total, "#e2e8f0")} Total Escolas:</span> 
                <b style="color: #ffffff;">${total}</b>
            </div>
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.urbana)} Urbanas:</span> 
                <b>${urbanas}</b>
            </div>
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.rural)} Rurais:</span> 
                <b>${rurais}</b>
            </div>
            
            <hr style="border-top: 1px solid #2d3748; margin: 12px 0; border-bottom: none;"/>
            
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.creche)} Creche/Pré:</span> 
                <b>${creche}</b>
            </div>
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.fund)} Ens. Fund:</span> 
                <b>${fund}</b>
            </div>
            <div class="info-row">
                <span style="display:flex; align-items:center;">${icon(paths.medio)} Ens. Médio:</span> 
                <b>${medio}</b>
            </div>
        `;
    };

    infoCardControl.addTo(map);
}

// Renderizar os markers.
function renderMarkers(data) {

    markerCluster.clearLayers();

    data.forEach(item => {
        if (!item.lat || !item.lon) return;

        const marker = L.marker([item.lat, item.lon]);

        marker.bindPopup(`
            <div class="custom-dark-popup">
                <b style="color:#0ea5e9;">${item.nomeEscola}</b><br/>
                <span style="color:#a0aec0; font-size:12px;">${item.endereco ?? "Endereço não disponível"}</span>
            </div>
        `);

        markerCluster.addLayer(marker);
    });
}


// Fazer update dos markers (fazer isso para evitar sobrecarga)
window.updateMarkers = (data) => {
    allMarkersData = data;
    renderMarkers(data);

};