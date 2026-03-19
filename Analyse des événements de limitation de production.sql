--  Les noms de tables, colonnes et valeurs ont été modifiés à des fins d'anonymisation
--  Cette requête est inspirée d'un cas réel dans le secteur de l'énergie
-- Analyse des événements de limitation de production sur des sites industriels 

SELECT
    'src-' || evt.id AS id_event,
    'src-' || evt.ref_site_id AS ref_site_id,
    
    COALESCE(reg.label, '---') AS region,
    COALESCE(ag.label, '---') AS agency,
    COALESCE(src.label, '---') AS source_station,
    COALESCE(dep.label, '---') AS network_segment,
    COALESCE(dep.code, '---') AS segment_code,
    
    COALESCE(loc.label, '---') AS delivery_point,
    COALESCE(loc.code, '---') AS delivery_code,
    
    prod.contract_number,
    prod.external_id,
    COALESCE(prod.site_name, '---') AS site_name,
    
    tech.production_type,
    
    evt.event_timestamp AS event_date,
    evt.limitation_rate AS limitation_rate,
    (evt.limitation_rate / 100.0) AS limitation_ratio,
    evt.limited_power,
    tech.max_power,
    
    evt.status,
    cause.short_label AS event_cause,

    -- Présence d’un dispositif spécifique
    CASE 
        WHEN ext.external_flag IS NULL THEN 'No'
        ELSE 'Yes'
    END AS external_device_present,

    -- Typologie d'événement
    CASE 
        WHEN cause.code IN ('A01','A03','A04','A05','A08','A09') THEN 'Incident'
        WHEN cause.code IN ('B02','B06','B10','B14') THEN 'Network Constraint'
        ELSE 'Maintenance'
    END AS event_type,

    -- Origine de la demande
    CASE 
        WHEN cause.code IN ('T01','T02','T03','B02','A03','A04','A05','B10') THEN 'External Operator'
        ELSE 'Internal Operator'
    END AS request_origin,

    -- Région simplifiée
    CASE 
        WHEN LEFT('src-' || evt.id, 3) = 'src' THEN 'Region_A'
    END AS region_group

FROM datawarehouse.production_events evt

INNER JOIN datawarehouse.production_sites prod 
    ON prod.site_id = evt.ref_site_id

INNER JOIN datawarehouse.production_units tech 
    ON tech.site_id = prod.id 

INNER JOIN datawarehouse.network_nodes loc 
    ON loc.node_id = tech.delivery_point_id

INNER JOIN datawarehouse.network_mapping map 
    ON map.node_id = loc.network_id

INNER JOIN datawarehouse.network_nodes dep 
    ON dep.node_id = map.parent_node_id 

INNER JOIN datawarehouse.network_sources src 
    ON src.source_id = dep.source_id

INNER JOIN datawarehouse.event_causes cause 
    ON evt.cause_id = cause.id

LEFT JOIN datawarehouse.network_assets asset 
    ON asset.asset_id = loc.asset_id

LEFT JOIN datawarehouse.intervention_zones zone 
    ON asset.zone_id = zone.id

LEFT JOIN datawarehouse.regions reg 
    ON zone.region_id = reg.id

LEFT JOIN datawarehouse.agencies ag 
    ON zone.agency_id = ag.id

LEFT JOIN datawarehouse.external_devices ext 
    ON ext.asset_id = asset.asset_id

WHERE 
    evt.status <> 'CANCELLED'
    AND evt.limitation_rate <> 100
    AND evt.event_timestamp >= '2020-01-01'
    AND cause.code NOT IN ('X09','X10','X16');