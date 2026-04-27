let map;
let markerCluster;
let baseYearMarkers = []; 
let allMarkersData = []; 
let levelStack = [];

let currentLevel = "meso";
let currentViewName = "Estado de Alagoas";
let currentAddress = "";
let currentDependency = "";
let currentLocality = "";
let currentCity = "";
let geoLayerGroup;

let currentFilterFn = null;
let currentMarkerFilterFn = null;
let currentDependencyFilter = null; 

let blazorRef = null;
let currentClickedLevel = "state";
let currentClickedId = null;

let infoCardControl;
let currentSummaryData = null;
let hoverCache = {};

const geoJsonUrls = {
    meso: "mesorregioes.json",
    micro: "microrregioes.json",
    municipio: "municipios.json"
};

window.updateBaseMarkers = (data) => {
    baseYearMarkers = data;
    applyFiltersAndRender();
};

window.applyDependencyFilter = (depId) => {
    currentDependencyFilter = depId;
    applyFiltersAndRender();
};

function applyFiltersAndRender() {
    let filteredData = baseYearMarkers;

    if (currentDependencyFilter !== null && currentDependencyFilter !== undefined) {
        filteredData = filteredData.filter(m => m.dependencia === currentDependencyFilter);
    }

    if (currentMarkerFilterFn) {
        filteredData = filteredData.filter(currentMarkerFilterFn);
    }

    allMarkersData = filteredData;
    renderMarkers(allMarkersData);
}

window.updateMarkers = (data) => {
    window.updateBaseMarkers(data);
};

window.updateMapSummary = (data) => {
    currentSummaryData = data;
    if (infoCardControl) infoCardControl.update();
};

window.updateHoverCache = (dataList) => {
    hoverCache = {};
    dataList.forEach(item => {
        const id = item.municipio_id || item.micro_id || item.meso_id;
        hoverCache[id] = item;
    });
};

function getStyle(level) {
    switch (level) {
        case "meso": return { color: "#e2e8f0", weight: 1.5, fillOpacity: 0.04, fillColor: "#f8fafc" };
        case "micro": return { color: "#cbd5e1", weight: 1.2, fillOpacity: 0.04, fillColor: "#f1f5f9" };
        case "municipio": return { color: "#94a3b8", weight: 0.8, fillOpacity: 0.04, fillColor: "#e2e8f0" };
    }
};

function handleFeatureClick(feature, level) {
    const rawId = feature.properties.id;
    const numericId = rawId ? Number(rawId) : null;
    const nome = feature.properties.nome || feature.properties.name;

    if (level === "meso") {
        navigateTo("micro", (f) => f.properties.meso_id == rawId, (m) => m.meso_id == numericId, "meso", numericId, `Mesorregião: ${nome}`);
    }
    else if (level === "micro") {
        currentCity = nome;
        navigateTo("municipio", (f) => f.properties.microrregiao?.micro_id == numericId, (m) => m.micro_id == numericId, "micro", numericId, `Microrregião: ${nome}`);
    }
    else if (level === "municipio") {
        currentCity = nome;
        navigateTo("municipio", (f) => f.properties.id == numericId, (m) => m.municipio_id == numericId, "municipio", numericId, `Município: ${nome}`);
    }
};

function navigateTo(level, filterFn, markerFilterFn, clickedLevel, clickedId, viewName, address = "", dependency = "", locality = "", city = "") {    
    levelStack.push({
        level: currentLevel,
        filter: currentFilterFn,
        markerFilter: currentMarkerFilterFn,
        clickedLevel: currentClickedLevel,
        clickedId: currentClickedId,
        viewName: currentViewName,
        address: currentAddress,
        dependency: currentDependency,
        locality: currentLocality,
        city: currentCity
    });

    currentLevel = level;
    currentFilterFn = filterFn;
    currentMarkerFilterFn = markerFilterFn;
    currentClickedLevel = clickedLevel;
    currentClickedId = clickedId;
    if (viewName) currentViewName = viewName;
    currentAddress = address;
    currentDependency = dependency;
    currentLocality = locality;
    currentCity = city;

    if (level !== "school") {
        loadGeoLayer(level, filterFn);
    }

    applyFiltersAndRender();

    if (blazorRef) {
        blazorRef.invokeMethodAsync('ApplyFilter', currentClickedLevel, currentClickedId);
    }

    if (infoCardControl) infoCardControl.update();
}

window.goBack = () => {
    if (levelStack.length === 0) return;

    const previous = levelStack.pop();

    currentLevel = previous.level;
    currentFilterFn = previous.filter;
    currentMarkerFilterFn = previous.markerFilter;
    currentClickedLevel = previous.clickedLevel;
    currentClickedId = previous.clickedId;
    currentViewName = previous.viewName;
    currentAddress = previous.address;
    currentDependency = previous.dependency;
    currentLocality = previous.locality;
    currentCity = previous.city;

    if (currentLevel !== "school") {
        loadGeoLayer(currentLevel, currentFilterFn);
    }

    applyFiltersAndRender();

    if (blazorRef) {
        blazorRef.invokeMethodAsync('ApplyFilter', currentClickedLevel, currentClickedId);
    }

    if (infoCardControl) infoCardControl.update();
};

async function loadGeoLayer(level, filterFn = null) {
    currentLevel = level;
    geoLayerGroup.clearLayers();
    const url = geoJsonUrls[level];
    const response = await fetch(url);
    let geojson = await response.json();

    if (filterFn) {
        geojson.features = geojson.features.filter(filterFn);
    }

    const layer = L.geoJSON(geojson, {
        style: getStyle(level),
        onEachFeature: (feature, layer) => {
            const nome = feature.properties.nome || feature.properties.name;
            layer.bindTooltip(`<b>${nome}</b>`, { sticky: true, className: 'custom-dark-tooltip' });
            layer.on("mouseover", function (e) {
                const targetLayer = e.target;
                targetLayer.setStyle({ fillOpacity: 0.25, color: "#ffffff", weight: 3 });
                infoCardControl.update(feature.properties);
            });
            layer.on("mouseout", function (e) {
                const targetLayer = e.target;
                targetLayer.setStyle(getStyle(level));
                infoCardControl.update();
            });
            layer.on("click", () => handleFeatureClick(feature, level));
        }
    }).addTo(map);

    geoLayerGroup.addLayer(layer);
    map.fitBounds(layer.getBounds(), { paddingTopLeft: [240, 20] });
};

window.initMap = async (geoJsonUrl, dotNetRef) => {
    blazorRef = dotNetRef;
    if (map) { map.remove() }

    map = L.map('map', { zoomControl: false }).setView([-9.66625, -35.7351], 8);

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; CARTO',
        subdomains: 'abcd',
        maxZoom: 20
    }).addTo(map);

    geoLayerGroup = L.layerGroup().addTo(map);

    markerCluster = L.markerClusterGroup({
        spiderfyOnMaxZoom: true,
        showCoverageOnHover: false,
        zoomToBoundsOnClick: true
    });
    map.addLayer(markerCluster);

    createInfoCard();

    // Se houver marcadores base carregados antes do initMap terminar
    if (baseYearMarkers && baseYearMarkers.length > 0) {
        applyFiltersAndRender();
    }

    await loadGeoLayer("meso");
};

function createInfoCard() {
    infoCardControl = L.control({ position: 'topleft' });
    infoCardControl.onAdd = function (map) {
        this._div = L.DomUtil.create('div', 'custom-map-info-card');
        this.update();
        return this._div;
    };

    infoCardControl.update = function (props) {
        console.log("Nível atual:", currentLevel);
        const icon = (path, color = "#94a3b8") => `<svg viewBox="0 0 24 24" fill="${color}" width="16" height="16" style="min-width: 16px; margin-right: 6px;"><path d="${path}"></path></svg>`;

        const paths = {
            total: "M12 3 1 9l4 2.18v6L12 21l7-3.82v-6l2.12-1.15V17h2V9L12 3zm6.82 6L12 12.72 5.18 9 12 5.28 18.82 9z",
            urbana: "M15 11V5l-3-3-3 3v2H3v14h18V11h-3zm-8 8H5v-2h2v2zm0-4H5v-2h2v2zm0-4H5V9h2v2zm6 8h-2v-2h2v2zm0-4h-2v-2h2v2zm0-4h-2V9h2v2zm0-4h-2V5h2v2zm6 12h-2v-2h2v2zm0-4h-2v-2h2v2z",
            rural: "m14 6-3.75 5 2.85 3.8-1.6 1.2C9.81 13.75 7 10 7 10l-6 8h22L14 6z",
            creche: "M22.94 12.66c.04-.21.06-.43.06-.66s-.02-.45-.06-.66c-.25-1.51-1.36-2.74-2.81-3.17-.53-1.12-1.28-2.1-2.19-2.91C16.36 3.85 14.28 3 12 3s-4.36.85-5.94 2.26c-.92.81-1.67 1.8-2.19 2.91-1.45.43-2.56 1.65-2.81 3.17-.04.21-.06.43-.06.66s.02.45.06.66c.25 1.51 1.36 2.74 2.81 3.17.52 1.11 1.27 2.09 2.17 2.89C7.62 20.14 9.71 21 12 21s4.38-.86 5.97-2.28c.9-.8 1.65-1.79 2.17-2.89 1.44-.43 2.55-1.65 2.8-3.17zM19 14c-.1 0-.19-.02-.29-.03-.2.67-.49 1.29-.86 1.86C16.6 17.74 14.45 19 12 19s-4.6-1.26-5.85-3.17c-.37-.57-.66-1.19-.86-1.86-.1.01-.19.03-.29.03-1.1 0-2-.9-2-2s.9-2 2-2c.1 0 .19.02.29.03.2-.67.49-1.29.86-1.86C7.4 6.26 9.55 5 12 5s4.6 1.26 5.85 3.17c.37.57.66 1.19.86 1.86.1-.01.19-.03.29-.03 1.1 0 2 .9 2 2s-.9 2-2 2z",
            fund: "M21 5c-1.11-.35-2.33-.5-3.5-.5-1.95 0-4.05.4-5.5 1.5-1.45-1.1-3.55-1.5-5.5-1.5S2.45 4.9 1 6v14.65c0 .25.25.5.5.5.1 0 .15-.05.25-.05C3.1 20.45 5.05 20 6.5 20c1.95 0 4.05.4 5.5 1.5 1.35-.85 3.8-1.5 5.5-1.5 1.65 0 3.35.3 4.75 1.05.1.05.15.05.25.05.25 0 .5-.25.5-.5V6c-.6-.45-1.25-.75-2-1zm0 13.5c-1.1-.35-2.3-.5-3.5-.5-1.7 0-4.15.65-5.5 1.5V8c1.35-.85 3.8-1.5 5.5-1.5 1.2 0 2.4.15 3.5.5v11.5z",
            medio: "M5 13.18v4L12 21l7-3.82v-4L12 17l-7-3.82zM12 3 1 9l11 6 9-4.91V17h2V9L12 3z",
            location: "M12 2C8.13 2 5 5.13 5 9c0 5.25 7 13 7 13s7-7.75 7-13c0-3.87-3.13-7-7-7z",
            net: "M12 7V3H2v18h20V7H12zM6 19H4v-2h2v2zm0-4H4v-2h2v2zm0-4H4V9h2v2zm0-4H4V5h2v2zm4 12H8v-2h2v2zm0-4H8v-2h2v2zm0-4H8V9h2v2zm0-4H8V5h2v2z"
        };

        if (currentLevel === "school") {
            this._div.innerHTML = `
            <span style="font-size: 11px; color: #38bdf8; text-transform: uppercase; font-weight: bold; letter-spacing: 0.5px;">Unidade Escolar</span><br/>
            <h4 style="color: #ffffff; margin: 4px 0 2px 0; font-size: 15px; line-height: 1.2;">${currentViewName}</h4>
            <span style="font-size: 10px; color: #94a3b8; display: block; margin-bottom: 12px; line-height: 1.3;">${currentAddress}</span>
            
            <div class="info-row" style="margin-bottom: 6px; display: flex; align-items: center;">
                ${icon(paths.net)} <span style="color: #94a3b8; font-size: 12px; margin-right: 4px;">Rede:</span> <b style="color: #fff; font-size: 12px;">${currentDependency}</b>
            </div>
            
            <div class="info-row" style="margin-bottom: 6px; display: flex; align-items: center;">
                ${icon(paths.location)} <span style="color: #94a3b8; font-size: 12px; margin-right: 4px;">Localização:</span> <b style="color: #fff; font-size: 12px;">${currentLocality}</b>
            </div>
            
            <div class="info-row" style="display: flex; align-items: center;">
                ${icon(paths.urbana)} <span style="color: #94a3b8; font-size: 12px; margin-right: 4px;">Município:</span> <b style="color: #fff; font-size: 12px;">${currentCity}</b>
            </div>
        `;
            return;
        }

        let headerHtml = "";
        let s = currentSummaryData;
        if (props) {
            const id = Number(props.id);
            if (hoverCache && hoverCache[id]) {
                s = hoverCache[id];
                let label = currentLevel === "meso" ? "Mesorregião" : currentLevel === "micro" ? "Microrregião" : "Município";
                headerHtml = `<span style="font-size: 11px; color: #38bdf8; text-transform: uppercase; font-weight: bold; letter-spacing: 0.5px;">Comparando: ${label}</span><br/><h4 style="color: #ffffff; margin: 4px 0 12px 0; font-size: 16px;">${props.nome || props.name}</h4>`;
            } else {
                headerHtml = `<span style="font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px;">Foco em</span><br/><h4 style="color: #38bdf8; margin: 4px 0 12px 0; font-size: 16px;">${props.nome || props.name}</h4>`;
            }
        } else {
            headerHtml = `<span style="font-size: 11px; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.5px;">Visão Geral</span><br/><h4 style="color: #e2e8f0; margin: 4px 0 12px 0; font-size: 16px;">${currentViewName}</h4>`;
        }

        if (!s) {
            this._div.innerHTML = `${headerHtml}<div class="info-row"><span>Carregando dados...</span></div>`;
            return;
        }

        this._div.innerHTML = `
            ${headerHtml}
            <div class="info-row">${icon(paths.total, "#e2e8f0")} Total Escolas: <b>${s.total_escolas}</b></div>
            <div class="info-row">${icon(paths.urbana)} Urbanas: <b>${s.total_escolas_urbanas}</b></div>
            <div class="info-row">${icon(paths.rural)} Rurais: <b>${s.total_escolas_rurais}</b></div>
            <hr style="border-top: 1px solid #2d3748; margin: 12px 0; border-bottom: none;"/>
            <div class="info-row">${icon(paths.creche)} Oferta Creche: <b>${(s.escolas_municipais_com_creche || 0) + (s.escolas_estaduais_com_creche || 0)}</b></div>
            <div class="info-row">${icon(paths.fund)} Oferta Fund: <b>${(s.escolas_municipais_com_fundamental || 0) + (s.escolas_estaduais_com_fundamental || 0)}</b></div>
            <div class="info-row">${icon(paths.medio)} Oferta Médio: <b>${(s.escolas_estaduais_com_medio || 0)}</b></div>`;
    };
    infoCardControl.addTo(map);
}

function renderMarkers(data) {
    if (!markerCluster) { allMarkersData = data; return; }
    markerCluster.clearLayers();

    data.forEach(item => {
        const lat = parseFloat(item.lat);
        const lon = parseFloat(item.lon);

        if (isNaN(lat) || isNaN(lon) || lat === 0 || lon === 0) return;

        const marker = L.marker([lat, lon]);
        marker.bindPopup(`<b style="color:#0ea5e9;">${item.escola_nome || item.nomeEscola}</b>`);

        marker.on('click', () => {
            const depLabel = item.dependencia === 2 ? "Estadual" : "Municipal";
            const locLabel = item.localizacao === 1 ? "Urbana" : "Rural";
            const cityLabel = item.nomeMunicipio || "Não informado";
            const idEscola = item.escola_id || item.idEscola;

            map.flyTo([lat, lon], 16);

            const schoolFilter = (m) => (m.escola_id || m.idEscola) == idEscola;

            navigateTo(
                "school",
                null,
                schoolFilter,
                "school",
                idEscola,
                item.escola_nome || item.nomeEscola,
                item.escola_endereco || item.endereco,
                depLabel,
                locLabel,
                cityLabel
            );
        });

        markerCluster.addLayer(marker);
    });
}

window.directSchoolNavigation = (id, nome, endereco, lat, lon, depId, locId, cityName) => {
    const dep = depId === 2 ? "Estadual" : "Municipal";
    const loc = locId === 1 ? "Urbana" : "Rural";
    const city = cityName || "Não informado";

    map.flyTo([lat, lon], 16);

    navigateTo("school", null, (m) => (m.escola_id || m.idEscola) == id, "school", id, nome, endereco, dep, loc, city);
};

window.directRegionNavigation = (level, id, nome) => {
    const feature = { properties: { id: id, nome: nome } };
    handleFeatureClick(feature, level);
};