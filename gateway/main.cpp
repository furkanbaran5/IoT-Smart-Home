#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include "mesh_protocol.h"

// ==================== AYARLAR ====================
const char *ssid = "Furkan";
const char *password = "12345678";

const char *mqtt_server = "3bbc547ace4f48ac8a185f2c1e92a5ae.s1.eu.hivemq.cloud";
const int mqtt_port = 8883;
const char *mqtt_user = "cabir";
const char *mqtt_pass = "Cabir123";

// ==================== KIMLIK ====================
const char *myId = "GW";
uint8_t myMac[6];
uint8_t broadcastAddress[] = {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
esp_now_peer_info_t peerInfo;

unsigned long lastSyncTransmission = 0;
volatile bool isAwayMode = false;

// ==================== RTOS ====================
#define QUEUE_LEN 10
QueueHandle_t packetQueue;
QueueHandle_t topologyQueue;
SemaphoreHandle_t neighborsMutex; // neighbors[] ve globalNodes[] icin mutex

// ==================== MQTT ====================
#include <PubSubClient.h>
#include <WiFiClientSecure.h>
WiFiClientSecure netClient;
PubSubClient mqtt(netClient);

// ==================== KOMSU TABLOSU ====================
NeighborNode neighbors[MAX_NEIGHBORS];
int neighborCount = 0;

void addOrUpdateNeighbor(const char *id, const uint8_t *mac, int linkRssi, int gwRssiFromPacket)
{
    xSemaphoreTake(neighborsMutex, portMAX_DELAY);
    for (int i = 0; i < neighborCount; i++)
    {
        if (strcmp(neighbors[i].nodeId, id) == 0)
        {
            neighbors[i].linkRssi = (int)(0.85f * neighbors[i].linkRssi + 0.15f * linkRssi);
            neighbors[i].gwRssi = gwRssiFromPacket;
            neighbors[i].lastSeen = millis();
            xSemaphoreGive(neighborsMutex);
            return;
        }
    }
    if (neighborCount < MAX_NEIGHBORS)
    {
        strcpy(neighbors[neighborCount].nodeId, id);
        memcpy(neighbors[neighborCount].macAddr, mac, 6);
        neighbors[neighborCount].linkRssi = linkRssi;
        neighbors[neighborCount].gwRssi = gwRssiFromPacket;
        neighbors[neighborCount].lastSeen = millis();
        neighborCount++;
        esp_now_peer_info_t p;
        memset(&p, 0, sizeof(p));
        memcpy(p.peer_addr, mac, 6);
        p.channel = 0;
        p.encrypt = false;
        if (!esp_now_is_peer_exist(mac))
            esp_now_add_peer(&p);
        Serial.printf("[YENI KOMSU] ID: %s | MAC: %02X:%02X:%02X:%02X:%02X:%02X | Link: %d dBm\n",
                      id, mac[0], mac[1], mac[2], mac[3], mac[4], mac[5], linkRssi);
    }
    xSemaphoreGive(neighborsMutex);
}

// ==================== GLOBAL NODE TABLOSU ====================
#define MAX_GLOBAL_NODES 30
struct GlobalNode
{
    char nodeId[10];
    char path[40];
    unsigned long lastSeen;
} globalNodes[MAX_GLOBAL_NODES];
int globalNodeCount = 0;

void updateGlobalNode(const char *id, const char *path)
{
    if (strcmp(id, "GW") == 0)
        return;
    xSemaphoreTake(neighborsMutex, portMAX_DELAY);
    for (int i = 0; i < globalNodeCount; i++)
    {
        if (strcmp(globalNodes[i].nodeId, id) == 0)
        {
            strcpy(globalNodes[i].path, path);
            globalNodes[i].lastSeen = millis();
            xSemaphoreGive(neighborsMutex);
            return;
        }
    }
    if (globalNodeCount < MAX_GLOBAL_NODES)
    {
        strcpy(globalNodes[globalNodeCount].nodeId, id);
        strcpy(globalNodes[globalNodeCount].path, path);
        globalNodes[globalNodeCount].lastSeen = millis();
        globalNodeCount++;
    }
    xSemaphoreGive(neighborsMutex);
}

void updateGlobalNodeAlive(const char *id)
{
    if (strcmp(id, "GW") == 0)
        return;
    xSemaphoreTake(neighborsMutex, portMAX_DELAY);
    for (int i = 0; i < globalNodeCount; i++)
    {
        if (strcmp(globalNodes[i].nodeId, id) == 0)
        {
            globalNodes[i].lastSeen = millis();
            break;
        }
    }
    xSemaphoreGive(neighborsMutex);
}

// ==================== PROMISCUOUS RSSI HACK ====================
volatile int latest_rssi = -50;
void promisc_cb(void *buf, wifi_promiscuous_pkt_type_t type)
{
    wifi_promiscuous_pkt_t *pkt = (wifi_promiscuous_pkt_t *)buf;
    if (pkt)
        latest_rssi = pkt->rx_ctrl.rssi;
}

// ==================== SYNC GONDERICI ====================
void sendSyncTo(const uint8_t *targetMac)
{
    MeshPacket pkt;
    memset(&pkt, 0, sizeof(pkt));
    strcpy(pkt.sender, myId);
    strcpy(pkt.receiver, "FFFF");
    memcpy(pkt.senderMac, myMac, 6);
    memcpy(pkt.destMac, myMac, 6);
    pkt.type = TYPE_SYNC;
    pkt.ttl = 2;
    pkt.gwRssi = 0;
    strcpy(pkt.path, "GW");
    snprintf(pkt.payload, sizeof(pkt.payload), "TICK|A%d", isAwayMode ? 1 : 0);
    esp_now_send(targetMac, (uint8_t *)&pkt, sizeof(MeshPacket));
}

// ==================== ALICI CALLBACK ====================
// WiFi gorevi (Core 0) tarafindan cagrilir.
// neighborsMutex ile thread-safe erisim saglanir.
void OnDataRecv(const uint8_t *mac, const uint8_t *incomingData, int len)
{
    if (len != sizeof(MeshPacket))
        return;
    int rssi = latest_rssi;
    MeshPacket pkt;
    memcpy(&pkt, incomingData, sizeof(MeshPacket));

    // Gateway kendi gonderdigi SYNC yankilarini yok say
    if (pkt.type == TYPE_SYNC)
        return;

    // --- ROTA PARSING (PATH PARSING) ---
    char tempPath[60];
    strncpy(tempPath, pkt.path, sizeof(tempPath) - 1);
    char *token = strtok(tempPath, ">");
    char lastRelay[15] = "";
    char prevToken[15] = "";

    while (token != NULL)
    {
        if (strcmp(token, "GW") == 0)
        {
            strcpy(lastRelay, prevToken);
        }
        else
        {
            // Yoldaki herhangi bir node'u "alive" olarak isaretle
            if (strcmp(token, pkt.sender) != 0)
            {
                updateGlobalNodeAlive(token);
            }
            strcpy(prevToken, token);
        }
        token = strtok(NULL, ">");
    }

    if (lastRelay[0] == '\0')
    {
        // Eger path'te GW yoksa veya ">" yoksa, demek ki tek node dogrudan gondermis
        strcpy(lastRelay, pkt.sender);
    }

    // Komsu (Neighbor) olarak asil kaynagi degil, paketi ulastiran son roleyi ekle
    addOrUpdateNeighbor(lastRelay, mac, rssi, pkt.gwRssi);
    updateGlobalNode(pkt.sender, pkt.path);

    if (pkt.type == TYPE_DISCOVERY)
    {
        Serial.printf("--> [DISCOVERY] %s agimiza katildi! Link RSSI: %d dBm\n", pkt.sender, rssi);
        sendSyncTo(mac);
        return;
    }

    if (pkt.type == TYPE_HEARTBEAT)
    {
        Serial.printf("[HB] %-8s | Link:%3d dBm | Rota: %s | %s\n",
                      pkt.sender, rssi, pkt.path, pkt.payload);
        // Heartbeat'i de MQTT kuyruğuna at — arayuz dashboard'da node'u gorsin
        MeshPacket *copy = (MeshPacket *)malloc(sizeof(MeshPacket));
        if (copy)
        {
            memcpy(copy, &pkt, sizeof(MeshPacket));
            if (xQueueSend(packetQueue, &copy, 0) != pdTRUE)
                free(copy);
        }
        return;
    }

    if (pkt.type == TYPE_EMERGENCY_ANOMALY)
    {
        Serial.printf("\n[!!! ANOMALI !!!] Kaynak: %s | Link RSSI: %d dBm\n", pkt.sender, rssi);
        Serial.printf("  Gecilen Rota : %s\n", pkt.path);
        Serial.printf("  Kaynak MAC   : %02X:%02X:%02X:%02X:%02X:%02X\n",
                      pkt.senderMac[0], pkt.senderMac[1], pkt.senderMac[2],
                      pkt.senderMac[3], pkt.senderMac[4], pkt.senderMac[5]);
        Serial.printf("  Veri         : %s\n\n", pkt.payload);
        MeshPacket *copy = (MeshPacket *)malloc(sizeof(MeshPacket));
        if (copy)
        {
            memcpy(copy, &pkt, sizeof(MeshPacket));
            if (xQueueSend(packetQueue, &copy, 0) != pdTRUE)
                free(copy);
        }
    }
}

void OnDataSent(const uint8_t *mac_addr, esp_now_send_status_t status) {}

// ==================== MQTT GOREVI (Core 1) ====================
void mqttCallback(char *topic, byte *payload, unsigned int length)
{
    String message;
    for (unsigned int i = 0; i < length; i++)
        message += (char)payload[i];
    Serial.printf("\n[MQTT ALINDI] Konu: %s | Mesaj: %s\n", topic, message.c_str());
    if (String(topic) == "ev/guvenlik/away_mode")
    {
        if (message.indexOf("true") > 0 || message.indexOf("1") > 0)
        {
            isAwayMode = true;
            Serial.println(">>> SISTEM: AWAY MODUNA GECILDI! PIR AKTIF.");
        }
        else
        {
            isAwayMode = false;
            Serial.println(">>> SISTEM: EVDEYIM MODUNA GECILDI! PIR IPTAL.");
        }
    }
}

void mqttTask(void *param)
{
    netClient.setInsecure();
    mqtt.setServer(mqtt_server, mqtt_port);
    mqtt.setCallback(mqttCallback);
    mqtt.setBufferSize(512);
    MeshPacket *p;
    while (true)
    {
        if (!mqtt.connected() && WiFi.status() == WL_CONNECTED)
        {
            String cid = "ESP32_GW_" + String(random(0xffff), HEX);
            if (mqtt.connect(cid.c_str(), mqtt_user, mqtt_pass))
            {
                Serial.println("[MQTT] Private Broker'a Baglandi!");
                mqtt.subscribe("ev/guvenlik/away_mode");
            }
            else
            {
                vTaskDelay(5000 / portTICK_PERIOD_MS);
                continue;
            }
        }
        mqtt.loop();
        if (xQueueReceive(packetQueue, &p, 50 / portTICK_PERIOD_MS) == pdTRUE)
        {
            if (mqtt.connected())
            {
                String json = String("{\"node\":\"") + p->sender + "\",\"path\":\"" + p->path + "\",\"data\":\"" + p->payload + "\"}";
                String topic = String("ev/guvenlik/anomali/") + p->sender;
                mqtt.publish(topic.c_str(), json.c_str());
                Serial.printf(">> [MQTT] Gonderildi: %s\n", json.c_str());
            }
            free(p);
        }
        char *tStr;
        if (xQueueReceive(topologyQueue, &tStr, 0) == pdTRUE)
        {
            if (mqtt.connected())
                mqtt.publish("ev/guvenlik/topology", tStr);
            free(tStr);
        }
    }
}

// ==================== SYNC GOREVI (Core 0) ====================
// Eskiden loop() icindeydi. Artik bagimsiz bir RTOS gorevi.
// neighborsMutex ile korunan tablolara guvenli erisir.
void syncTask(void *param)
{
    while (true)
    {
        unsigned long now = millis();
        if (now - lastSyncTransmission >= CYCLE_PERIOD_MS)
        {
            lastSyncTransmission = now;

            // Komsu MAC adreslerini mutex ile guvenli kopyala
            xSemaphoreTake(neighborsMutex, portMAX_DELAY);
            int nc = neighborCount;
            uint8_t macList[MAX_NEIGHBORS][6];
            for (int i = 0; i < nc; i++)
                memcpy(macList[i], neighbors[i].macAddr, 6);
            xSemaphoreGive(neighborsMutex);

            if (nc == 0)
            {
                sendSyncTo(broadcastAddress);
                Serial.println("[ZAMAN_MASTER] Kimse yok, Broadcast SYNC atildi.");
            }
            else
            {
                for (int i = 0; i < nc; i++)
                    sendSyncTo(macList[i]);
                Serial.printf("[ZAMAN_MASTER] %d komsuya Unicast SYNC atildi.\n", nc);
            }

            // Komsu tablosunu yazdir (Kullanici istegi: Butun agi/globalNodes goster)
            Serial.println("\n\u250c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2510");
            Serial.println("\u2502           AGDAKI TUM CIHAZLAR (GLOBAL NODES)            \u2502");
            Serial.println("\u2502  Cihaz    | Son Gorulme  | Rota                         \u2502");
            Serial.println("\u251c\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2524");
            xSemaphoreTake(neighborsMutex, portMAX_DELAY);
            if (globalNodeCount == 0)
                Serial.println("\u2502  Bos.                                                            \u2502");
            for (int i = 0; i < globalNodeCount; i++)
            {
                long sn = (millis() - globalNodes[i].lastSeen) / 1000;
                Serial.printf("\u2502 %-8s | %3lds once    | %-28s \u2502\n",
                              globalNodes[i].nodeId, sn, globalNodes[i].path);
            }
            xSemaphoreGive(neighborsMutex);
            Serial.println("\u2514\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2518\n");

            // Topoloji JSON olustur ve gonder
            xSemaphoreTake(neighborsMutex, portMAX_DELAY);
            if (globalNodeCount > 0)
            {
                String topo = "{\"type\":\"topology\",\"nodes\":[";
                for (int i = 0; i < globalNodeCount; i++)
                {
                    long sn = (millis() - globalNodes[i].lastSeen) / 1000;
                    topo += "{\"id\":\"" + String(globalNodes[i].nodeId) + "\",\"path\":\"" + String(globalNodes[i].path) + "\",\"lastSeenSec\":" + String(sn) + "}";
                    if (i < globalNodeCount - 1)
                        topo += ",";
                }
                topo += "]}";
                char *topoCopy = (char *)malloc(topo.length() + 1);
                if (topoCopy)
                {
                    strcpy(topoCopy, topo.c_str());
                    if (xQueueSend(topologyQueue, &topoCopy, 0) != pdTRUE)
                        free(topoCopy);
                }
            }
            xSemaphoreGive(neighborsMutex);
        }
        vTaskDelay(10 / portTICK_PERIOD_MS);
    }
}

// ==================== SETUP ====================
void setup()
{
    Serial.begin(115200);
    delay(1000);
    Serial.println("\n=== GATEWAY (MASTER CLOCK + MQTT ROUTER | FreeRTOS) ===");

    packetQueue = xQueueCreate(QUEUE_LEN, sizeof(MeshPacket *));
    topologyQueue = xQueueCreate(3, sizeof(char *));
    neighborsMutex = xSemaphoreCreateMutex();

    WiFi.mode(WIFI_STA);
    WiFi.macAddress(myMac);
    Serial.println("\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557");
    Serial.println("\u2551           CIHAZ MAC ADRESI           \u2551");
    Serial.printf("\u2551  >> %02X:%02X:%02X:%02X:%02X:%02X <<        \u2551\n",
                  myMac[0], myMac[1], myMac[2], myMac[3], myMac[4], myMac[5]);
    Serial.println("\u2551  Rol: GATEWAY (FreeRTOS)             \u2551");
    Serial.println("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n");

    WiFi.begin(ssid, password);
    Serial.print("Wi-Fi Agina Baglaniliyor");
    int t = 0;
    while (WiFi.status() != WL_CONNECTED && t < 20)
    {
        delay(500);
        Serial.print(".");
        t++;
    }

    if (WiFi.status() == WL_CONNECTED)
    {
        int ch = WiFi.channel();
        Serial.printf("\n[TAMAM]: Wi-Fi Baglandi! Kanal: %d\n", ch);
        esp_wifi_set_channel(ch, WIFI_SECOND_CHAN_NONE);
    }
    else
    {
        Serial.println("\n[UYARI]: Wi-Fi Baglanamadi! Mesh Kanal 1'de baslatiliyor.");
        WiFi.disconnect();
        esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE);
    }

    esp_wifi_set_promiscuous_rx_cb(&promisc_cb);
    esp_wifi_set_promiscuous(true);

    if (esp_now_init() != ESP_OK)
    {
        Serial.println("[HATA]: ESP-NOW baslatilamadi!");
        return;
    }
    esp_now_register_recv_cb(OnDataRecv);
    esp_now_register_send_cb(OnDataSent);

    memcpy(peerInfo.peer_addr, broadcastAddress, 6);
    peerInfo.channel = 0;
    peerInfo.encrypt = false;
    esp_now_add_peer(&peerInfo);

    // RTOS Gorevleri — her biri ayri core'a pinlendi
    xTaskCreatePinnedToCore(mqttTask, "MQTTTask", 4096, NULL, 1, NULL, 1); // Core 1
    xTaskCreatePinnedToCore(syncTask, "SyncTask", 4096, NULL, 2, NULL, 0); // Core 0
    Serial.println("[HAZIR] FreeRTOS gorevleri basladi.");
}

// ==================== ANA DONGU ====================
// loop() artik kullanilmiyor.
// Tum isler syncTask (Core 0) ve mqttTask (Core 1)'e devredildi.
void loop()
{
    vTaskDelay(portMAX_DELAY);
}
