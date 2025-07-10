/*
 * Copyright 2024 Ryan Gregg (ryan@ryangregg.com).
 * Licensed under Apache 2 license. See LICENSE for more information.
 */

const app_name = "signalk-mqtt-sensors";
const mqtt = require('mqtt');
const jsonpath = require('jsonpath')
const crypto = require('crypto');

console.log(`loading ${app_name}`);

/**
 * The types of sensors supported by this plugin
 */
const SensorType = Object.freeze({
    TEMPERATURE: "temperature",
    WATER_LEAK: "water_leak",
    HUMIDITY: "humidity",
    BATTERY: "battery",
    PRESSURE: "pressure",
    OTHER: "other"
});

module.exports = function(app) {
    const plugin = {};
    const hasRegisteredWithHomeAssistant = new Map();
    const writablePaths = new Map(); // Map of SignalK path -> {commandTopic, sensor}

    plugin.id = app_name;
    plugin.name = "MQTT Sensors";
    plugin.description = "Signal K node server plugin for mapping values between MQTT and Signal K topics";

    // Handle PUT requests to writable paths
    plugin.handleMessage = function(path, value, callback) {
        app.debug(`Received PUT request to path: ${path} with value: ${value}`);
        
        const writableConfig = writablePaths.get(path);
        if (!writableConfig) {
            app.debug(`Path ${path} is not configured as writable`);
            callback(new Error(`Path ${path} is not writable or not configured`));
            return;
        }

        if (!plugin.mqttClient || !plugin.mqttClient.connected) {
            app.debug('MQTT client is not connected, cannot publish command');
            callback(new Error('MQTT client is not connected'));
            return;
        }

        const { commandTopic, sensor } = writableConfig;
        
        // Validate the value type matches the sensor type
        if (!isValidValueForSensor(value, sensor)) {
            app.debug(`Invalid value type for sensor ${sensor.sensor}: ${typeof value}`);
            callback(new Error(`Invalid value type for sensor type ${sensor.sensor}`));
            return;
        }

        // Publish the value to the command topic
        let payload = value;
        if (typeof value === 'object') {
            payload = JSON.stringify(value);
        }

        plugin.mqttClient.publish(commandTopic, payload.toString(), { qos: 1 }, (err) => {
            if (err) {
                app.debug(`Error publishing command to topic ${commandTopic}:`, err.message);
                callback(new Error(`Failed to publish command: ${err.message}`));
            } else {
                app.debug(`Successfully published command to topic ${commandTopic}: ${payload}`);
                callback(null, {
                    state: 'COMPLETED',
                    statusCode: 200
                });
            }
        });
    };

    /**
     * Validates if a value is appropriate for the given sensor type
     * @param {any} value - The value to validate
     * @param {Object} sensor - The sensor configuration
     * @returns {boolean} True if the value is valid for the sensor type
     */
    function isValidValueForSensor(value, sensor) {
        const sensorType = sensor.sensor;
        const unit = sensor.unit;
        
        // Only allow writable for number and boolean types
        if (sensorType === SensorType.TEMPERATURE || sensorType === SensorType.PRESSURE || 
            sensorType === SensorType.HUMIDITY || sensorType === SensorType.BATTERY) {
            return typeof value === 'number' && !isNaN(value);
        }
        
        if (sensorType === SensorType.WATER_LEAK || unit === 'boolean') {
            return typeof value === 'boolean';
        }
        
        if (sensorType === SensorType.OTHER) {
            // For 'other' type, allow numbers and booleans only
            return typeof value === 'number' || typeof value === 'boolean';
        }
        
        return false;
    }

    // define the option schema for the add-on
    plugin.schema =  require("./schema.json");
    plugin.uiSchema = require("./uischema.json");

    plugin.mqttClient = null;

    /**
     * Starts the plugin and connects to the MQTT server
     * @param {Object} options - The configuration options
     */
    plugin.start = function(options) {
        console.log(`Starting ${app_name}`);
        
        const fromTopics = loadFromTopics(options);
        const toTopics = loadToTopics(options);

        app.debug("Updating server with SignalK metadata units");
        publishDataTypesToServer(app, fromTopics);

        // Connect to the MQTT server and start listening for changes
        // to the topics we're interested in
                
        if (options.enabled)
        {
            app.debug("Connecting to MQTT server...");
            plugin.mqttClient = connectToMqttServer(options, fromTopics);

            if (toTopics.enabled) {
                app.debug("Connecting to SignalK deltas...");
                subscribeToDeltas(app, toTopics, plugin.mqttClient);
            }
        }

        /**
         * Subscribes to SignalK delta updates for specified paths and publishes them to MQTT topics.
         * Handles Home Assistant discovery and rate limiting of updates.
         * @param {object} app - The SignalK server app instance
         * @param {object} toTopics - Configuration object containing MQTT publishing settings
         * @param {MqttClient} mqttClient - The MQTT client instance used for publishing
         */
        function subscribeToDeltas(app, toTopics, mqttClient) {
            const homeAssistantDiscoveryEnabled = toTopics.home_assistant_discovery || false;
            const lastPublishedTimestamps = new Map(); // Store the last published timestamps for each path
            const rateLimit = (typeof toTopics.min_publish_interval === 'number' ? toTopics.min_publish_interval : 0) * 1000;
            const baseTopic = toTopics.base_topic;
            toTopics.paths.forEach(path => {
                app.streambundle.getSelfStream(path).onValue(value => {
                    //app.debug(`Delta received for ${path}:`, value);
            
                    const currentTime = Date.now();
                    const lastPublishedTime = lastPublishedTimestamps.get(path) || 0;

                    if (homeAssistantDiscoveryEnabled) 
                    {
                        // The first time we encounter a value - register it with Home Assistant
                        publishHomeAssistantDiscovery(app, mqttClient, toTopics.base_topic, path);
                    }
            
                    // Check if enough time has elapsed since the last update
                    if ((currentTime - lastPublishedTime) >= rateLimit) {
                        publishDeltaToMqtt(mqttClient, baseTopic, path, value);
                        lastPublishedTimestamps.set(path, currentTime); // Update the last published timestamp
                    } else {
                        //app.debug(`Skipping update for ${path}: Minimum time interval not reached.`);
                    }
                });
            
                // Publish the current value to MQTT when starting (rate limit is not applied here)
                const value = app.getSelfPath(path);
                if (value !== undefined) {
                    publishDeltaToMqtt(mqttClient, baseTopic, path, value);
                    lastPublishedTimestamps.set(path, Date.now()); // Initialize the last published timestamp
                }
            })
        }

        /**
         * Publishes a SignalK delta value to an MQTT topic
         * @param {MqttClient} mqttClient - The MQTT client instance used for publishing
         * @param {string} baseTopic - The base MQTT topic to publish to
         * @param {string} path - The SignalK path of the value
         * @param {any} value - The value to publish
         */
        function publishDeltaToMqtt(mqttClient, baseTopic, path, value) {
            if (!mqttClient) {
                app.debug("MQTT client is not connected, cannot publish.");
                return;
            }

            const topic = `${baseTopic}/${path}`;
        
            let payload;
            try {
                // Check if value is already a JSON string
                const parsedValue = JSON.parse(value);
                // If we get here, it's valid JSON, so include it directly
                payload = {
                    path: path,
                    value: parsedValue
                };
            } catch {
                // Not JSON, treat as regular value
                payload = {
                    path: path,
                    value: value
                };
            }
            payload = JSON.stringify(payload);
        
            mqttClient.publish(topic, payload, { qos: 1 }, (err) => {
                if (err) {
                    app.debug(`Error publishing to topic ${topic}:`, err.message);
                } else {
                    app.debug(`Published to topic ${topic}: ${payload}`);
                }
            });
        }

        /**
         * Establishes a connection to the MQTT server and sets up event handlers
         * @param {Object} options - Configuration options including mqtt_server, mqtt_username, and mqtt_password
         * @param {Array<string>} fromTopics - Array of MQTT topics to subscribe to
         * @returns {MqttClient} The connected MQTT client instance
         */
        function connectToMqttServer(options, fromTopics) {
            const client_options = {
                username: options.mqtt_username,
                password: options.mqtt_password,
                reconnectPeriod: 5000, // Auto reconnect every 5 seconds
                connectTimeout: 30000  // Connection timeout of 30 seconds
            };
    
            const mqttClient = mqtt.connect(options.mqtt_server, client_options);
            mqttClient.on('connect', () => {
                console.log(`Connected to MQTT broker: ${options.mqtt_server}`);
                setStatus(`Connected to ${options.mqtt_server}`, false);
                
                // Reset reconnection state if we were reconnecting
                if (mqttClient.reconnecting) {
                    mqttClient.reconnecting = false;
                    console.log('MQTT reconnection successful');
                }

                // list topics to subscribe to
                const topics = identifyTopicsForSubscription(fromTopics);
                app.debug("MQTT topics for subscription", topics)

                mqttClient.subscribe(topics, (err) => {
                    if (err) {
                        console.warn(`Error subscribing to topics: ${err}`);
                    }
                    else {
                        app.debug("Subscribed to all required topics");
                    }
                });
            });

            // Event: Disconnected
            mqttClient.on('disconnect', (packet) => {
                console.warn('MQTT client disconnected:', packet);
                setStatus("Disconnected", false);
            });

            // Event: Reconnect attempt
            mqttClient.on('reconnect', () => {
                console.log('Attempting to reconnect to MQTT broker...');
                setStatus("Attempting to reconnect to MQTT broker...", false);
            });

            // Event: Connection closed
            mqttClient.on('close', () => {
                console.log('MQTT connection closed');
                setStatus("Connection closed.", false);
            });

            // Event: Error
            mqttClient.on('error', (err) => {
                console.error('MQTT error:', err.message);
                setStatus(`Connection error: ${err.message}`, true);
                
                // Implement reconnection logic with exponential backoff
                if (!mqttClient.reconnecting) {
                    mqttClient.reconnecting = true;
                    let retryCount = 0;
                    const maxRetries = 20;
                    const reconnectWithBackoff = () => {
                        if (retryCount < maxRetries) {
                            const delay = Math.min(30000, Math.pow(2, retryCount) * 1000); // Exponential backoff capped at 30 seconds
                            retryCount++;
                            console.log(`MQTT reconnect attempt ${retryCount}/${maxRetries} in ${delay/1000} seconds...`);
                            setTimeout(() => {
                                if (!mqttClient.connected) {
                                    console.log('Attempting to reconnect to MQTT broker...');
                                    mqttClient.reconnect();
                                } else {
                                    mqttClient.reconnecting = false;
                                }
                            }, delay);
                        } else {
                            console.error('Maximum MQTT reconnection attempts reached.');
                            setStatus('Maximum reconnection attempts reached. Please check MQTT server.', true);
                            mqttClient.reconnecting = false;
                        }
                    };
                    reconnectWithBackoff();
                }
            });

            // Event: Message received
            mqttClient.on('message', (topic, message) => {
                app.debug(`Received MQTT message: ${message.toString()} on topic: ${topic}`);

                let deltas = processMqttMessage(topic, message.toString());
                const data = {
                    updates: [
                      {
                        values: deltas
                      }
                    ]
                };

                if (data) {
                    app.debug("Updating app with deltas: ", data)
                    app.handleMessage(plugin.id, data);
                }
            });

            return mqttClient;
        }

        function getTopic(fromTopics, search) {
            for (const topic of fromTopics) {
                if (topic.mqtt_topic === search) {
                    return topic; // This returns the topic and exits the function
                }
            }
            return null; // Return null if no match is found
        }

        /**
         * Processes an MQTT message and converts it into Signal K delta format.
         * 
         * @param {string} topic - The MQTT topic the message was received on
         * @param {string} value - The message payload received from MQTT. Can be either a raw value
         *                        or a JSON string if json_path is specified in the sensor config
         * @returns {Array} An array of Signal K delta objects ready to be sent to the server, or
         *                 undefined if no matching topic configuration is found
         * 
         * The function:
         * 1. Looks up the topic configuration from fromTopics
         * 2. For each sensor configured for that topic:
         *    - If a json_path is specified, extracts the value using that path
         *    - Otherwise uses the raw value
         * 3. Converts the value into Signal K delta format using prepareDelta()
         */
        function processMqttMessage(topic, value) {
            const config = getTopic(fromTopics, topic)
            if (null == config)
            {
                app.debug("Couldn't find a registration for MQTT topic", topic);
                return;
            }

            app.debug("Processing topic", topic)

            let deltas = [];
            config.sensors.forEach(sensor => {
                // Parse the sensor to see if we have the data we need for it
                let parsedValue = value;
                app.debug("Parsing value", value);
                
                if (sensor.json_path != null) {
                    // Parse the response as JSON and retrive the value from the path
                    try
                    {
                        const jsonObject = JSON.parse(value);
                        const result = jsonpath.query(jsonObject, sensor.json_path);
                        if (result && result.length > 0) {
                            parsedValue = result[0];
                            app.debug(`Parsed ${sensor.json_path} to find value ${parsedValue}`);
                        } else {
                            app.debug(`JSON path ${sensor.json_path} did not return any results.`);
                            return; // Skip this sensor if no value is found
                        }
                    } catch (error) {
                        app.debug("Error finding value from JSON path", error)
                        return;
                    }
                }
                const delta = prepareDelta(sensor, parsedValue);
                deltas.push(delta);
            });

            app.debug("Found deltas: ", deltas);
            return deltas;
        }

        /**
         * Converts the value from the MQTT message into the expected Signal K format
         * @param {Object} sensor - The sensor configuration object
         * @param {any} parsedValue - The value parsed from the MQTT message
         * @returns {Object} The Signal K delta object
         */
        function prepareDelta(sensor, parsedValue) {
            // Do any conversions necessary between the input value defined by the sensor
            // parameters and the expected output value for that data type in Signal K.

            const type = sensor.sensor;
            const unit = sensor.unit;
            const signalk_path = sensor.destination;
            let value = null;
            app.debug(`Preparing delta for ${type} with unit ${unit} to path ${signalk_path}`);
            switch(type) {
                case SensorType.TEMPERATURE:
                    if (unit == "F") {
                        // Convert from F to K
                        value = (Number(parsedValue) - 32) / 1.8 + 273.15;
                        value = parseFloat(value.toFixed(2)); // Rounds to 2 decimal points
                        app.debug(`Converting temperature from F to K: ${value}`);
                    } else if (unit == "C") {
                        // Convert from C to K
                        value = Number(parsedValue) + 273.15;
                        value = parseFloat(value.toFixed(2)); // Rounds to 2 decimal points
                        app.debug(`Converting temperature from C to K: ${value}`);                        
                    }
                    else if (unit == "K") {
                        app.debug("No data conversion necessary");
                        value = Number(parsedValue);
                    }
                    break;
                case SensorType.PRESSURE:
                    switch (unit) {
                        case "Pa":
                            app.debug("No data conversion necessary");
                            value = Number(parsedValue);
                            break;
                        case "hPa":
                            app.debug(`Converting pressure from hPa to Pa: ${value}`)
                            value = Number(parsedValue) * 100.0;
                            break;
                        case "mmHg":
                            app.debug(`Converting pressure from mmHg to Pa: ${value}`)
                            value = Number(parsedValue) * 133.322;
                            break;
                        case "atm":
                            app.debug(`Converting pressure from atm to Pa: ${value}`)
                            value = Number(parsedValue) * 101325.0;
                            break;
                        default:
                            app.debug(`Unknown conversion from ${unit} to SI unit for pressure.`);
                            value = parsedValue;
                            break;
                    }
                    break;
                default:
                    app.debug(`No data type conversation for ${type}`);
                    value = parsedValue;
            }
            return {
                path: signalk_path,
                value: value
            }
        }

        /**
         * Extracts and deduplicates MQTT topics from configuration objects
         * @param {Array} fromTopics - Array of objects from the 'from' field in mqtt-sensors.yaml
         * @returns {Array<string>} Array of unique MQTT topic strings
         * @throws {Error} If fromTopics is not an array
         */
        function identifyTopicsForSubscription(fromTopics) {
            if (!Array.isArray(fromTopics)) {
                throw new Error("Invalid input: fromTopics must be an array");
            }
        
            // Use a Set to ensure uniqueness of topics
            const uniqueTopics = new Set();
        
            fromTopics.forEach((topicObj) => {
                if (topicObj.mqtt_topic) {
                    uniqueTopics.add(topicObj.mqtt_topic);
                }
            });

            // Convert the Set back to an array
            return Array.from(uniqueTopics);
        }

        /**
         * Returns the SI unit for a given sensor type
         * @param {string} sensorType - The type of sensor
         * @returns {string} The SI unit for the sensor type
         */
        function getSIUnit(sensorType) {
            switch (sensorType) {
                case SensorType.TEMPERATURE:
                    return "K"; // Kelvin
                case SensorType.HUMIDITY:
                    return "%"; // Percentage
                case SensorType.BATTERY:
                    return "%"; // Percentage
                case SensorType.PRESSURE:
                    return "Pa"; // Pascal
                case SensorType.WATER_LEAK:
                    return "boolean"; // Boolean for presence/absence
                case SensorType.OTHER:
                default:
                    return null; // Or a sensible default like an empty string
            }
        }

        /**
         * Returns the data type for a given sensor configuration
         * @param {Object} sensor - The sensor configuration object
         * @returns {string} The data type (number, boolean, or string)
         */
        function getDataType(sensor) {
            const sensorType = sensor.sensor;
            const unit = sensor.unit;
            
            // Determine type based on unit first, then sensor type
            if (unit === "boolean") {
                return "boolean";
            }
            
            if (unit === "string" || unit === "literal") {
                return "string";
            }
            
            // For numeric sensor types
            if (sensorType === SensorType.TEMPERATURE || 
                sensorType === SensorType.PRESSURE || 
                sensorType === SensorType.HUMIDITY || 
                sensorType === SensorType.BATTERY) {
                return "number";
            }
            
            // Water leak is typically boolean
            if (sensorType === SensorType.WATER_LEAK) {
                return "boolean";
            }
            
            // For "other" type, infer from unit
            if (sensorType === SensorType.OTHER) {
                // If it's a numeric unit, return number
                if (["C", "F", "K", "percent", "ratio", "Pa", "hPa", "mmHg"].includes(unit)) {
                    return "number";
                }
                // Default to string for unknown units in "other" type
                return "string";
            }
            
            // Default fallback
            return "string";
        }        

        /**
         * Publishes the data types to the server
         * @param {Object} app - The SignalK server app instance
         * @param {Array} fromTopics - Array of objects from the 'from' field in mqtt-sensors.yaml
         */
        function publishDataTypesToServer(app, fromTopics) {
            app.debug('Updating data types with server', fromTopics);

            let meta = [];
            fromTopics.forEach(topic => {
                const sensors = topic.sensors;
                sensors.forEach(sensor => {
                    app.debug('Discovering data for sensor', sensor);
                    const path = sensor.destination;
                    var unit = getSIUnit(sensor.sensor);
                    
                    // Build metadata object
                    let metadataValue = {};
                    
                    // Always emit units - use "unitless" when no units are defined
                    metadataValue.units = unit || "unitless";
                    
                    // Add type information based on sensor configuration
                    metadataValue.type = getDataType(sensor);
                    
                    // Check if this path is writable and add writable metadata
                    if (sensor.writable && sensor.command_topic) {
                        metadataValue.writable = true;
                        app.debug(`Marking path ${path} as writable in metadata`);
                    }
                    
                    // Always add metadata since we now emit type and units for all properties
                    meta.push({
                        path: path,
                        value: metadataValue
                    });

                })
            })

            if (meta.length > 0) {
                app.debug('Publishing meta data types', meta)
                app.handleMessage(plugin.id, {
                    updates: [{
                        meta: meta
                    }]
                });
            } else {
                app.debug("No deltas were created for this notification. Nothing updated");
            }
        }

        /**
         * Loads the MQTT sensor definitions from the configuration file
         * @param {Object} options - The configuration options
         * @returns {Array} An array of sensor definitions
         */
        function loadFromTopics(options) {
            const from = options.from;

            if (!Array.isArray(from))
                return [];

            // Clear previous writable paths
            writablePaths.clear();

            app.debug("Loading MQTT sensor definitions...")
            from.forEach( (topic) => {
                app.debug("MQTT Topic: ", topic);
                app.debug("  Defined Sensors:");
                topic.sensors.forEach( (sensor) => {
                    app.debug(`${JSON.stringify(sensor)}`);
                    
                    // Register writable sensors
                    if (sensor.writable && sensor.command_topic) {
                        const path = sensor.destination;
                        writablePaths.set(path, {
                            commandTopic: sensor.command_topic,
                            sensor: sensor
                        });
                        app.debug(`Registered writable path: ${path} -> ${sensor.command_topic}`);
                    }
                });
            });

            return from;
        }

        /**
         * Loads the MQTT sensor definitions from the configuration file
         * @param {Object} options - The configuration options
         * @returns {Object} The MQTT sensor definitions
         */
        function loadToTopics(options) {
            const to = options.to;
            if (to == undefined) {
                return { 
                    enabled: false,
                    base_topic: "",
                    paths: []
                };
            }
            
            const paths = to.paths;
            if (!Array.isArray(paths)) {
                return { 
                    enabled: false,
                    base_topic: "",
                    paths: []
                };
            }

            app.debug("Loading export sensors...");
            paths.forEach( (topic) => {
                app.debug(`Export: Path: ${topic.signalk_path} to Topic: ${topic.mqtt_topic}`);
            });
            return to;
        }

        const { version } = require('./package.json');

        /**
         * Publishes a Signal K path to the Home Assistant discovery topic
         * @param {Object} app - The SignalK server app instance
         * @param {MqttClient} mqttClient - The MQTT client instance used for publishing
         * @param {string} baseTopic - The base MQTT topic to publish to
         * @param {string} path - The SignalK path of the value
         */
        function publishHomeAssistantDiscovery(app, mqttClient, baseTopic, path) {
            
            const isRegistered = hasRegisteredWithHomeAssistant.get(path);
            if (isRegistered) {
                return;
            }

            app.debug("Home Assistant: generating discovery for path", path);
            const metadata = app.getSelfPath(path + ".meta");
            if (metadata == undefined) {
                app.debug(`Path ${path} is not defined in Signal K - skipped.`);
                return;
            }

            hasRegisteredWithHomeAssistant.set(path, true);
            app.debug("Signal K Data for", path, metadata);

            if (metadata.properties != undefined) {
                // There are multiple properties we need to emit, not just a single value
                for (let key in metadata.properties) {
                    registerHomeAssistantEntity(metadata.properties[key], path, baseTopic, app, mqttClient, "{{ value_json.value." + key + " }}");
                };
            } else {
                registerHomeAssistantEntity(metadata, path, baseTopic, app, mqttClient, null);
            }
            
            app.debug("Discovery enabled for path", path);
        }

        function registerHomeAssistantEntity(metadata, path, baseTopic, app, mqttClient, value_template) {
            if (!metadata) {
                app.debug(`Metadata for path ${path} is null or undefined. Skipping registration.`);
                return;
            }
            let displayName = convertPathToDisplayName(path);
            if (value_template != null && metadata.description) {
                displayName += ` ${metadata.description}`;
            }
            
            const deviceClass = determineDeviceClass(path, metadata);
            if (!deviceClass) {
                app.debug(`No device class determined for path ${path}. Proceeding without device class.`);
            }
            
            const uniqueId = generateUUIDFromPath(path + value_template);
            const stateClass = "measurement"; // Default state class for sensors

            const configPayload = {
                device: {
                    identifiers: ["41c0ad04-4bcd-425a-b749-8bf4554d4cf4"],
                    manufacturer: "signalk-sensors",
                    model: "signalk-sensors",
                    name: "SignalK Sensors",
                    sw_version: version
                },
                name: displayName,
                state_class: stateClass,
                device_class: deviceClass,
                unit_of_measurement: metadata.units || null,
                unique_id: uniqueId,
                state_topic: `${baseTopic}/${path}`,
                value_template: value_template || "{{ value_json.value }}"
            };

            // Ensure device_class is null if it's an empty string
            if (configPayload.device_class === "") {
                configPayload.device_class = null;
            }

            app.debug("Registering HA entity for path: ", configPayload);

            // Publish the discovery message to the correct topic
            const component = "sensor"; // Change this based on the type of device (e.g., binary_sensor)
            const objectId = configPayload.unique_id;
            const discoveryTopic = `homeassistant/${component}/mqtt-sensors-${objectId}/config`;

            mqttClient.publish(
                discoveryTopic,
                JSON.stringify(configPayload),
                { retain: true, qos: 1 },
                (err) => {
                    if (err) {
                        app.debug(`Failed to publish HA discovery config for ${path}: ${err.message}`);
                    } else {
                        app.debug(`Published HA discovery config for ${path} to ${discoveryTopic}`);
                    }
                }
            );
        }

        /**
         * Determines the device class for a given Signal K path
         * @param {string} path - The SignalK path
         * @param {Object} metadata - The metadata object for the path
         * @returns {string} The device class - must be one of the following:
         * 'date', 'enum', 'timestamp', 'apparent_power', 'aqi', 'area', 'atmospheric_pressure', 'battery', 'blood_glucose_concentration', 'carbon_monoxide', 'carbon_dioxide', 'conductivity', 'current', 'data_rate', 'data_size', 'distance', 'duration', 'energy', 'energy_storage', 'frequency', 'gas', 'humidity', 'illuminance', 'irradiance', 'moisture', 'monetary', 'nitrogen_dioxide', 'nitrogen_monoxide', 'nitrous_oxide', 'ozone', 'ph', 'pm1', 'pm10', 'pm25', 'power_factor', 'power', 'precipitation', 'precipitation_intensity', 'pressure', 'reactive_power', 'signal_strength', 'sound_pressure', 'speed', 'sulphur_dioxide', 'temperature', 'volatile_organic_compounds', 'volatile_organic_compounds_parts', 'voltage', 'volume', 'volume_storage', 'volume_flow_rate', 'water', 'weight', 'wind_speed'
         */
        function determineDeviceClass(path, metadata) {
            if (!metadata) {
                app.debug("No metadata was available to determine device class");
                return null; // Default if metadata is unavailable
            }

            app.debug("processing device class for path", path);
        
            const unit = metadata.units?.toLowerCase();
            const displayName = metadata.displayName?.toLowerCase();
        
            // Map units to device_class
            if (unit === "v") return "voltage";
            if (unit === "a") return "current";
            if (unit === "%") {
                if (path.includes("humidity")) return "humidity";
                if (path.includes("battery")) return "battery";
                return "battery"
            }
            if (unit === "k" || unit === "°c" || unit === "°f") return "temperature";
            if (["pa", "hpa", "atm"].includes(unit)) return "pressure";
            if (unit === "boolean") {
                if (path.includes("water") || displayName.includes("leak")) return "moisture";
                return "problem";
            }
        
            // Fallback for other cases based on path or displayName
            if (path.includes("voltage")) return "voltage";
            if (path.includes("amperage") || path.includes("current")) return "current";
            if (path.includes("temperature")) return "temperature";
        
            app.debug(`Unhandled metadata for determining device class: ${path} - ${JSON.stringify(metadata)}`);
            return null; // Default if no match is found
        }

        /**
         * Converts a Signal K path to a display name.
         * @param {string} path - The Signal K path (e.g., "electrical.batteries.house.voltage").
         * @returns {string} - The human-readable display name (e.g., "Electrical Batteries House Voltage").
         */
        function convertPathToDisplayName(path) {
            if (!path || typeof path !== "string") {
                throw new Error("Invalid path: Path must be a non-empty string.");
            }

            // Split the path into components using the '.' delimiter
            const components = path.split('.');

            // Capitalize the first letter of each component and join them with spaces
            const displayName = components
                .map(component => component.charAt(0).toUpperCase() + component.slice(1))
                .join(' ');

            return displayName;
        }

        /**
         * Generates a UUID based on the hash of the input string.
         * @param {string} input - The input string to hash (e.g., a Signal K path).
         * @returns {string} - The generated UUID.
         */
        function generateUUIDFromPath(input) {
            if (!input || typeof input !== "string") {
                throw new Error("Invalid input: must be a non-empty string.");
            }

            // Hash the input string using SHA-1
            const hash = crypto.createHash('sha1').update(input).digest('hex');

            // Format the hash into a UUID
            const uuid = [
                hash.slice(0, 8),        // 8 characters
                hash.slice(8, 12),       // 4 characters
                `4${hash.slice(12, 15)}`, // 4 characters, 4 indicates version 4 UUID
                `${(parseInt(hash[16], 16) & 0x3 | 0x8).toString(16)}${hash.slice(17, 20)}`, // 4 characters
                hash.slice(20, 32)       // 12 characters
            ].join('-');

            return uuid;
        }
    }

    /**
     * Stops the plugin and disconnects from the MQTT server
     */
    plugin.stop = function() {
        app.debug(`${app_name} is stopping`);
        if (plugin.mqttClient && typeof plugin.mqttClient.end === 'function') {
            plugin.mqttClient.end();
            plugin.mqttClient = null;
        }
        // Clear the registration map
        hasRegisteredWithHomeAssistant.clear();
        // Clear writable paths
        writablePaths.clear();
    }

    /**
     * Sets the status message for the plugin
     * @param {string} statusMessage - The status message
     * @param {boolean} isError - Whether the status message is an error
     */
    function setStatus(statusMessage, isError) {
        if (isError) {
            app.setPluginError(statusMessage)
            app.debug(`Error: ${statusMessage}`);
        } else {
            app.setPluginStatus(statusMessage);
        }
    }

    return plugin;
}
