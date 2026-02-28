import { useEffect, useRef, useState } from "react";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import "leaflet.markercluster";
import "leaflet.markercluster/dist/MarkerCluster.css";
import "leaflet.markercluster/dist/MarkerCluster.Default.css";
import { apiFetch } from "../api/client";
import "../stylesheets/base.css";
import "../stylesheets/map.css";

const RESORT_MARKER_ICON = L.divIcon({
  className: "single-resort-marker-icon",
  html: '<div class="single-resort-marker-dot" aria-hidden="true"></div>',
  iconSize: [30, 30],
  iconAnchor: [15, 15],
  popupAnchor: [0, -15],
});

const DEFAULT_CENTER = [46.8, 8.2];
const DEFAULT_ZOOM = 5;
const LIFTS_MIN_ZOOM = 9;
const SLOPES_MIN_ZOOM = 10;
const CLUSTER_PIXEL_RADIUS = 55;

function toNumberOrNull(value) {
  if (value == null) return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function getResortCoordinates(resort) {
  const latitude = toNumberOrNull(resort.geography?.coordinates?.latitude ?? resort.latitude);
  const longitude = toNumberOrNull(resort.geography?.coordinates?.longitude ?? resort.longitude);

  if (latitude == null || longitude == null) return null;

  return [latitude, longitude];
}

function getSlopeColor(difficulty) {
  const key = String(difficulty ?? "").toLowerCase().trim();

  if (key === "green") return "#27ae60";
  if (key === "blue") return "#2980b9";
  if (key === "red") return "#c0392b";
  if (key === "black") return "#2c3e50";

  return "#7f8c8d";
}

function createLineEntry(startLat, startLon, endLat, endLon, style) {
  const aLat = toNumberOrNull(startLat);
  const aLon = toNumberOrNull(startLon);
  const bLat = toNumberOrNull(endLat);
  const bLon = toNumberOrNull(endLon);

  if (aLat == null || aLon == null || bLat == null || bLon == null) {
    return null;
  }

  const layer = L.polyline(
    [
      [aLat, aLon],
      [bLat, bLon],
    ],
    style
  );

  return {
    layer,
    bounds: L.latLngBounds(
      [aLat, aLon],
      [bLat, bLon]
    ),
  };
}

function createResortPopupHtml(resort) {
  return (
    `<strong>${resort.name ?? "Unknown resort"}</strong><br/>` +
    `${resort.geography?.country ?? resort.country ?? "N/A"}`
  );
}

function createResortMarker(resort) {
  return L.marker(resort.resortLatLng, { icon: RESORT_MARKER_ICON }).bindPopup(resort.popupHtml);
}

function splitVisibleResortsByProximity(visibleResorts, map) {
  const groupedIndexes = new Set();
  const zoom = map.getZoom();
  const projected = visibleResorts.map((resort) => map.project(resort.resortLatLng, zoom));

  for (let i = 0; i < projected.length; i += 1) {
    for (let j = i + 1; j < projected.length; j += 1) {
      if (projected[i].distanceTo(projected[j]) <= CLUSTER_PIXEL_RADIUS) {
        groupedIndexes.add(i);
        groupedIndexes.add(j);
      }
    }
  }

  const singles = [];
  const grouped = [];

  for (let i = 0; i < visibleResorts.length; i += 1) {
    if (groupedIndexes.has(i)) {
      grouped.push(visibleResorts[i]);
    } else {
      singles.push(visibleResorts[i]);
    }
  }

  return { singles, grouped };
}

function prepareResort(resort) {
  const coordinates = getResortCoordinates(resort);
  const resortLatLng = coordinates ? L.latLng(coordinates[0], coordinates[1]) : null;
  const popupHtml = createResortPopupHtml(resort);

  const lifts = (resort.lifts ?? [])
    .map((lift) =>
      createLineEntry(
        lift.lat_start,
        lift.lon_start,
        lift.lat_end,
        lift.lon_end,
        {
          color: "#8e8e8e",
          weight: 2,
          opacity: 0.8,
        }
      )
    )
    .filter(Boolean);

  const slopes = (resort.slopes ?? [])
    .map((slope) =>
      createLineEntry(
        slope.lat_start,
        slope.lon_start,
        slope.lat_end,
        slope.lon_end,
        {
          color: getSlopeColor(slope.difficulty),
          weight: 2.2,
          opacity: 0.95,
        }
      )
    )
    .filter(Boolean);

  return {
    resortLatLng,
    popupHtml,
    lifts,
    slopes,
  };
}

export default function Map() {
  const containerRef = useRef(null);
  const mapRef = useRef(null);
  const clusterMarkersRef = useRef(null);
  const singleMarkersRef = useRef(null);
  const liftsLayerRef = useRef(null);
  const slopesLayerRef = useRef(null);
  const preparedResortsRef = useRef([]);

  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    if (mapRef.current || !containerRef.current) return;

    const map = L.map(containerRef.current, {
      center: DEFAULT_CENTER,
      zoom: DEFAULT_ZOOM,
      minZoom: 2,
      worldCopyJump: true,
    });

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
      maxZoom: 19,
    }).addTo(map);

    const clusterMarkers = L.markerClusterGroup({
      showCoverageOnHover: false,
      spiderfyOnMaxZoom: true,
      maxClusterRadius: CLUSTER_PIXEL_RADIUS,
    });

    const singleMarkers = L.layerGroup();
    const liftsLayer = L.layerGroup();
    const slopesLayer = L.layerGroup();

    clusterMarkers.addTo(map);
    singleMarkers.addTo(map);
    liftsLayer.addTo(map);
    slopesLayer.addTo(map);

    mapRef.current = map;
    clusterMarkersRef.current = clusterMarkers;
    singleMarkersRef.current = singleMarkers;
    liftsLayerRef.current = liftsLayer;
    slopesLayerRef.current = slopesLayer;

    return () => {
      map.off("moveend");
      map.off("zoomend");
      map.remove();
      mapRef.current = null;
      clusterMarkersRef.current = null;
      singleMarkersRef.current = null;
      liftsLayerRef.current = null;
      slopesLayerRef.current = null;
      preparedResortsRef.current = [];
    };
  }, []);

  useEffect(() => {
    async function loadMapData() {
      if (
        !mapRef.current ||
        !clusterMarkersRef.current ||
        !singleMarkersRef.current ||
        !liftsLayerRef.current ||
        !slopesLayerRef.current
      ) {
        return;
      }

      setLoading(true);
      setError("");

      try {
        const resorts = await apiFetch("/resorts");
        preparedResortsRef.current = resorts.map(prepareResort);

        const bounds = L.latLngBounds([]);
        for (const resort of preparedResortsRef.current) {
          if (resort.resortLatLng) bounds.extend(resort.resortLatLng);
        }

        if (bounds.isValid()) {
          mapRef.current.fitBounds(bounds.pad(0.15));
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Map data could not be loaded.");
      } finally {
        setLoading(false);
      }
    }

    loadMapData();
  }, []);

  useEffect(() => {
    const map = mapRef.current;
    const clusterMarkers = clusterMarkersRef.current;
    const singleMarkers = singleMarkersRef.current;
    const liftsLayer = liftsLayerRef.current;
    const slopesLayer = slopesLayerRef.current;
    if (!map || !clusterMarkers || !singleMarkers || !liftsLayer || !slopesLayer) return;

    const refreshVisibleLayers = () => {
      const visibleBounds = map.getBounds().pad(0.1);
      const zoom = map.getZoom();
      const showLifts = zoom >= LIFTS_MIN_ZOOM;
      const showSlopes = zoom >= SLOPES_MIN_ZOOM;

      clusterMarkers.clearLayers();
      singleMarkers.clearLayers();
      liftsLayer.clearLayers();
      slopesLayer.clearLayers();

      const visibleResorts = preparedResortsRef.current.filter(
        (resort) => resort.resortLatLng && visibleBounds.contains(resort.resortLatLng)
      );
      const { singles, grouped } = splitVisibleResortsByProximity(visibleResorts, map);

      for (const resort of grouped) {
        clusterMarkers.addLayer(createResortMarker(resort));
      }

      for (const resort of singles) {
        singleMarkers.addLayer(createResortMarker(resort));
      }

      for (const resort of preparedResortsRef.current) {
        if (showLifts) {
          for (const lift of resort.lifts) {
            if (visibleBounds.intersects(lift.bounds)) {
              liftsLayer.addLayer(lift.layer);
            }
          }
        }

        if (showSlopes) {
          for (const slope of resort.slopes) {
            if (visibleBounds.intersects(slope.bounds)) {
              slopesLayer.addLayer(slope.layer);
            }
          }
        }
      }
    };

    refreshVisibleLayers();
    map.on("moveend", refreshVisibleLayers);
    map.on("zoomend", refreshVisibleLayers);

    return () => {
      map.off("moveend", refreshVisibleLayers);
      map.off("zoomend", refreshVisibleLayers);
    };
  }, [loading]);

  return (
    <div className="page-container map-page">
      <h1>Ski Map</h1>
      <p className="map-subtitle">
        Interactive skimap to find resorts around the world!
      </p>

      <div className="map-frame">
        <div ref={containerRef} className="ski-map-canvas" />
      </div>

      {loading && <p className="map-status">Lade Kartendaten...</p>}
      {error && <p className="map-status map-error">Fehler: {error}</p>}
    </div>
  );
}
