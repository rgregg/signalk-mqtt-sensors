/*
 * Copyright 2024 Ryan Gregg (ryan@ryangregg.com)
 */

const app_name = "signalk-mqtt-sensors";
const mqtt = require('mqtt');
const jsonpath = require('jsonpath')

console.log(`loading ${app_name}`);

const SensorType = Object.freeze({
    TEMPERATURE: "temperature",
    WATER_LEAK: "water_leak",
    HUMIDITY: "humidity",
    BATTERY: "battery",
    PRESSURE: "pressure",
    OTHER: "other"
});

module.exports = function(app) {
    var plugin = {};

    plugin.id = app_name;
    plugin.name = "MQTT Sensors"
    plugin.description = "Signal K node server plugin for mapping values between MQTT and Signal K topics";

    // define the option schema for the add-on
    plugin.schema =  require("./schema.json")
    plugin.uiSchema = require("./uischema.json")

    var mqttClient = null;

    // start method when the plugin is started. Options will
    // match the schema specified in the schema provided above.
    plugin.start = function(options, restartPluginFunc) {
        console.log(`Staring ${app_name}`);
        
        const fromTopics = loadFromTopics(options);

        app.debug("Updating server with SignalK metadata units");
        publishDataTypesToServer(app, fromTopics);

        // Connect to the MQTT server and start listening for changes
        // to the topics we're interested in
                
        if (options.enabled)
        {
            connectToMqttServer(options, fromTopics);
        }

        function connectToMqttServer(options, fromTopics) {
            const client_options = {
                username: options.mqtt_username,
                password: options.mqtt_password
            };
    
            mqttClient = mqtt.connect(options.mqtt_server, client_options);
            mqttClient.on('connect', () => {
                console.log(`Connected to MQTT broker: ${options.mqtt_server}`);
                setStatus(`Connected to ${options.mqtt_server}`, false);

                // list topics to subscribe to
                topics = identifyTopicsForSubscription(fromTopics);
                app.debug("MQTT topics for subscription", topics)

                mqttClient.subscribe(topics, (err, granted) => {
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
                setStatus("Attemping to reconnect to MQTT broker...", false);
            });

            // Event: Connection closed
            mqttClient.on('close', () => {
                console.log('MQTT connection closed');
                setStatus("Connect closed.", false);
            });

            // Event: Error
            mqttClient.on('error', (err) => {
                console.error('MQTT error:', err.message);
                // Handle critical errors here if needed
                setStatus(`Connection error: ${err.message}`, true);
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
        }

        function getTopic(fromTopics, search) {
            for (const topic of fromTopics) {
                if (topic.mqtt_topic === search) {
                    return topic; // This returns the topic and exits the function
                }
            }
            return null; // Return null if no match is found
        }

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
                        default:
                            app.debug(`Unknown conversion from ${unit} to SI unit for pressure.`);
                            value = parsedValue;
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

        // Return an array of strings with the unique values
        // of the mqtt_topics parameter from the objects in the from field
        // in the mqtt-sensors.yaml file
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

        function publishDataTypesToServer(app, fromTopics) {
            // TODO: We need to convert the Signal K paths in the 
            // fromTopics tree into their respective data units for
            // the Signal K server and submit these updates.

            app.debug('Updating data types with server', fromTopics);

            meta = [];
            fromTopics.forEach(topic => {
                const sensors = topic.sensors;
                sensors.forEach(sensor => {
                    app.debug('Discovering data for sensor', sensor);
                    const path = sensor.destination;
                    var unit = getSIUnit(sensor.sensor);
                    if (unit) {
                        meta.push({
                            path: path,
                            value: { units: unit }
                        });
                    }

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

        function loadFromTopics(options) {
            const from = options.from;

            if (!Array.isArray(from))
                return [];

            app.debug("Loading MQTT sensor definitions...")
            from.forEach( (topic, index) => {
                app.debug("MQTT Topic: ", topic);
                app.debug("  Defined Sensors:");
                topic.sensors.forEach( (sensor, sensorIndex) => {
                    app.debug(`${JSON.stringify(sensor)}`)
                });
            });

            /*
            {
                mqtt_topic: 'zigbee2mqtt/Front Cabin Water Sensor',
                sensors: [
                    {
                        json_path: '$.device_temperature',
                        destination: 'environment.inside.cabinFront.temperature',
                        sensor: 'temperature',
                        unit: 'C'
                    },
                    {
                        json_path: '$.water_leak',
                        destination: 'environment.inside.cabinFront.water_leak',
                        sensor: 'water_leak',
                        unit: 'boolean'
                    }
               ]
            }
         */

            return from;
        }
    }

    plugin.stop = function() {
        app.debug(`${app_name} is stopping`);
        if (mqttClient) {
            mqttClient.end()
        }
    }

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
