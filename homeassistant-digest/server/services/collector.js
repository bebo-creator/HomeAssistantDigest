const { getAllStates } = require('./homeassistant');
const { getMonitoredEntities } = require('../db/entities');
const { addSnapshots, deleteOldSnapshots, getSnapshotStats } = require('../db/snapshots');

let isCollecting = false;
let lastCollectionTime = null;
let collectionErrors = [];

/**
 * Collect snapshots from all monitored entities
 */
async function collectSnapshots() {
    if (isCollecting) {
        console.log('Collection already in progress, skipping...');
        return { skipped: true };
    }

    isCollecting = true;
    collectionErrors = [];
    const startTime = Date.now();

    try {
        const monitoredEntities = getMonitoredEntities()
            .filter(e => e.priority !== 'ignore');

        if (monitoredEntities.length === 0) {
            console.log('No entities to monitor');
            return { collected: 0, skipped: false };
        }

        const allStates = await getAllStates();
        const stateMap = new Map(allStates.map(s => [s.entity_id, s]));

        const timestamp = new Date().toISOString();
        const snapshots = [];

        for (const entity of monitoredEntities) {

            const state = stateMap.get(entity.entity_id);

            if (!state) {
                collectionErrors.push({
                    entity_id: entity.entity_id,
                    error: 'Entity not found in HA states'
                });
                continue;
            }

            if (state.state === 'unavailable' || state.state === 'unknown') {
                continue;
            }

            const numValue = parseFloat(state.state);
            const isNumeric = !isNaN(numValue) && isFinite(numValue);

            const relevantAttrs = extractRelevantAttributes(
                entity.domain,
                state.attributes
            );

            snapshots.push({
                entity_id: entity.entity_id,
                timestamp,
                value_type: isNumeric ? 'number' : 'state',
                value_num: isNumeric ? numValue : null,
                value_str: isNumeric ? null : state.state,
                attributes: Object.keys(relevantAttrs).length > 0 ? relevantAttrs : null
            });
        }

        if (snapshots.length > 0) {
            addSnapshots(snapshots);
        }

        lastCollectionTime = new Date();
        const duration = Date.now() - startTime;

        console.log(`Collected ${snapshots.length} snapshots in ${duration}ms`);

        return {
            collected: snapshots.length,
            errors: collectionErrors.length,
            duration,
            skipped: false
        };

    } catch (error) {
        console.error('Collection failed:', error);
        collectionErrors.push({ error: error.message });
        throw error;
    } finally {
        isCollecting = false;
    }
}

/**
 * Extract only relevant attributes to minimize storage
 */
function extractRelevantAttributes(domain, attributes) {

    const relevant = {};

    const attrMap = {

        climate: [
            'current_temperature',
            'temperature',
            'hvac_mode',
            'hvac_action',
            'preset_mode',
            'ema_temp'
        ],

        sensor: [
            'device_class',
            'unit_of_measurement'
        ],

        binary_sensor: [
            'device_class'
        ],

        light: [
            'brightness',
            'color_temp',
            'rgb_color'
        ],

        switch: [],

        cover: [
            'current_position'
        ],

        media_player: [
            'media_title',
            'media_artist',
            'volume_level'
        ],

        weather: [
            'temperature',
            'humidity',
            'pressure',
            'wind_speed'
        ]
    };

    const keysToExtract = attrMap[domain] || [];

    for (const key of keysToExtract) {
        if (attributes[key] !== undefined) {
            relevant[key] = attributes[key];
        }
    }

    /*
    --- Virtial Thermostat specific data ---
    */

    if (domain === 'climate') {

        if (attributes.ext_current_temperature !== undefined) {
            relevant.ext_current_temperature = attributes.ext_current_temperature;
        }

        if (attributes.total_energy !== undefined) {
            relevant.total_energy = attributes.total_energy;
        }

        if (attributes.temperature_slope !== undefined) {
            relevant.temperature_slope = attributes.temperature_slope;
        }

        const vt = attributes.vtherm_over_switch;

        if (vt) {

            if (vt.on_percent !== undefined)
                relevant.on_percent = vt.on_percent;

            if (vt.power_percent !== undefined)
                relevant.power_percent = vt.power_percent;

            if (vt.on_time_sec !== undefined)
                relevant.on_time_sec = vt.on_time_sec;

            if (vt.off_time_sec !== undefined)
                relevant.off_time_sec = vt.off_time_sec;

            if (vt.function !== undefined)
                relevant.function = vt.function;

            if (vt.tpi_coef_int !== undefined)
                relevant.tpi_coef_int = vt.tpi_coef_int;

            if (vt.tpi_coef_ext !== undefined)
                relevant.tpi_coef_ext = vt.tpi_coef_ext;

            if (vt.tpi_threshold_low !== undefined)
                relevant.tpi_threshold_low = vt.tpi_threshold_low;

            if (vt.tpi_threshold_high !== undefined)
                relevant.tpi_threshold_high = vt.tpi_threshold_high;

            if (vt.minimal_activation_delay !== undefined)
                relevant.minimal_activation_delay = vt.minimal_activation_delay;

            if (vt.minimal_deactivation_delay !== undefined)
                relevant.minimal_deactivation_delay = vt.minimal_deactivation_delay;

        }

    }

    return relevant;
}

/**
 * Clean up old snapshots beyond retention period
 */
async function cleanupOldData() {

    const historyDays = parseInt(process.env.HISTORY_DAYS) || 7;

    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - historyDays);

    const cutoffISO = cutoffDate.toISOString();

    const deletedCount = deleteOldSnapshots(cutoffISO);

    if (deletedCount > 0) {
        console.log(`Cleaned up ${deletedCount} old snapshots (before ${cutoffISO})`);
    }

    return {
        deleted: deletedCount,
        cutoff: cutoffISO
    };
}

/**
 * Get collector status
 */
function getCollectorStatus() {

    const stats = getSnapshotStats();

    return {
        isCollecting,
        lastCollectionTime: lastCollectionTime?.toISOString() || null,
        recentErrors: collectionErrors.slice(-10),
        stats
    };
}

module.exports = {
    collectSnapshots,
    cleanupOldData,
    getCollectorStatus
};
