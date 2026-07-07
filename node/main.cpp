/*
 * SLAVE SENSOR - WROOM (Multi-Hop Routing v4 - FreeRTOS)
 * =========================================================
 * Gorev Yapisi:
 *   sensorTask (Core 0, Priority 1):
 *       Her zaman calisir. Anten kapali olsa bile DHT11 ve PIR olcumu yapar.
 *       Olcumleri windowMutex ile korunan pencere dizilerine yazar.
 *
 *   meshTask (Core 1, Priority 2):
 *       SYNC bekleme, paket gonderme, WiFi (anten) yonetimi.
 *       syncSemaphore ile SYNC geldiginde uyandiriliyor.
 *       "Uyku" suresi: sadece anten kapaniyor, sensorTask calismaya devam ediyor.
 */

#include <Arduino.h>
#include <WiFi.h>
#include <esp_now.h>
#include <esp_wifi.h>
#include <math.h>
#include "mesh_protocol.h"
#include <DHTesp.h>

#define DHTPIN 13
#define PIRPIN 12

DHTesp dht;
volatile bool pirTriggered = false;

void IRAM_ATTR pir_isr() {
    pirTriggered = true;
}

// ==================== AYARLAR ====================
#define WIFI_CHANNEL  1
#define WINDOW_SIZE  30

float diffWindow[WINDOW_SIZE];
float tempWindow[WINDOW_SIZE];
float humWindow[WINDOW_SIZE];
int   pirWindow[WINDOW_SIZE];
int   windowCount = 0;
int   windowIdx   = 0;
float prevValue   = -999;
int   totalCount  = 0;

volatile bool anomaliTetikte = false;
volatile bool pirTetikte     = false;

// ==================== KIMLIK ====================
char    myId[10]  = "N_UNKN";
uint8_t myMac[6];
uint8_t broadcastAddress[] = {0xFF,0xFF,0xFF,0xFF,0xFF,0xFF};
esp_now_peer_info_t peerInfo;

// ==================== KOMSU TABLOSU ====================
NeighborNode neighbors[MAX_NEIGHBORS];
int neighborCount = 0;

// ==================== SENKRONIZASYON ====================
volatile bool          isSynced         = false;
volatile unsigned long lastSyncTime     = 0;
unsigned long          lastProcessTime  = 0;
volatile unsigned long lastDirectGwSyncTime = 0;
volatile unsigned long lastRelaySyncTime    = 0;
volatile int           knownChannel     = 0;
int                    discoveryAttemptCount = 0;
int                    missedSyncCount  = 0;
volatile bool          relayedThisCycle = false;
volatile bool          isAwayMode       = false;

uint8_t gateway_mac[6];
bool    hasGatewayMac = false;
volatile int myGwRssi = -127;

// ── Routing Hysteresis Sabitleri ─────────────────────────────
#define LOOP_GUARD_DB      3   // dBm — loop engelleyici esik
#define HOP_HYSTERESIS_DB  8   // dBm — rota degistirme direnci

char  currentHopId[10]    = "";
float currentHopScore     = -999.0f;

// ==================== RTOS ====================
SemaphoreHandle_t syncSemaphore;   // SYNC paketi gelince meshTask'i uyandiran binary semafor
SemaphoreHandle_t windowMutex;     // tempWindow/humWindow/pirWindow/diffWindow icin mutex
TaskHandle_t      sensorTaskHandle;
TaskHandle_t      meshTaskHandle;

// ==================== PROMISCUOUS RSSI ====================
volatile int latest_rssi = -50;
void promisc_cb(void* buf, wifi_promiscuous_pkt_type_t type) {
    wifi_promiscuous_pkt_t* pkt = (wifi_promiscuous_pkt_t*)buf;
    if (pkt) latest_rssi = pkt->rx_ctrl.rssi;
}

// ==================== KOMSU TABLOSU ====================
void addOrUpdateNeighbor(const char* id, const uint8_t* mac, int linkRssi, int gwRssi, const char* path = "") {
    for (int i = 0; i < neighborCount; i++) {
        if (strcmp(neighbors[i].nodeId, id) == 0) {
            neighbors[i].linkRssi = (int)(0.85f * neighbors[i].linkRssi + 0.15f * linkRssi);
            neighbors[i].gwRssi   = gwRssi;
            neighbors[i].lastSeen = millis();
            if (path && strlen(path) > 0) strncpy(neighbors[i].path, path, sizeof(neighbors[i].path)-1);
            return;
        }
    }
    if (neighborCount < MAX_NEIGHBORS) {
        strcpy(neighbors[neighborCount].nodeId, id);
        memcpy(neighbors[neighborCount].macAddr, mac, 6);
        neighbors[neighborCount].linkRssi = linkRssi;
        neighbors[neighborCount].gwRssi   = gwRssi;
        neighbors[neighborCount].lastSeen = millis();
        if (path && strlen(path) > 0) strncpy(neighbors[neighborCount].path, path, sizeof(neighbors[neighborCount].path)-1);
        else neighbors[neighborCount].path[0] = '\0';
        neighborCount++;
        esp_now_peer_info_t p; memset(&p,0,sizeof(p));
        memcpy(p.peer_addr,mac,6); p.channel=0; p.encrypt=false;
        if (!esp_now_is_peer_exist(mac)) esp_now_add_peer(&p);
        Serial.printf("[YENI KOMSU] %-8s | %02X:%02X:%02X:%02X:%02X:%02X | Link:%d GW:%d\n",
                      id,mac[0],mac[1],mac[2],mac[3],mac[4],mac[5],linkRssi,gwRssi);
    }
}

// ==================== UNICAST SYNC RELAY ====================
void relaySyncUnicast(const uint8_t* targetMac) {
    if (!hasGatewayMac || !isSynced) return;
    MeshPacket relay; memset(&relay, 0, sizeof(relay));
    strcpy(relay.sender,   myId);
    strcpy(relay.receiver, "FFFF");
    memcpy(relay.senderMac, myMac, 6);
    memcpy(relay.destMac, gateway_mac, 6);
    relay.type   = TYPE_SYNC;
    relay.ttl    = 0;
    relay.gwRssi = (int16_t)myGwRssi;
    snprintf(relay.path, sizeof(relay.path), "GW>%s", myId);
    snprintf(relay.payload, sizeof(relay.payload), "RELAY_TICK|A%d", isAwayMode ? 1 : 0);
    esp_now_send(targetMac, (uint8_t*)&relay, sizeof(MeshPacket));
    Serial.printf("[SYNC-RELAY->] Uzak node'a Unicast SYNC gonderildi | gwRssi=%d\n", myGwRssi);
}

// ==================== BROADCAST SYNC RELAY ====================
void relaySyncBroadcast(const uint8_t* gw_mac, int rssiToGw, uint8_t ttl) {
    if (ttl == 0) return;
    MeshPacket relay; memset(&relay, 0, sizeof(relay));
    strcpy(relay.sender, myId); strcpy(relay.receiver, "FFFF");
    memcpy(relay.senderMac, myMac, 6); memcpy(relay.destMac, gw_mac, 6);
    relay.type=TYPE_SYNC; relay.ttl=ttl-1; relay.gwRssi=(int16_t)rssiToGw;
    snprintf(relay.path, sizeof(relay.path), "GW>%s", myId);
    snprintf(relay.payload, sizeof(relay.payload), "RELAY_TICK|A%d", isAwayMode ? 1 : 0);
    vTaskDelay(pdMS_TO_TICKS(random(10, 60))); // Collsion onlemek icin random gecikme
    esp_now_send(broadcastAddress, (uint8_t*)&relay, sizeof(MeshPacket));
}

// ==================== ALICI CALLBACK ====================
// WiFi gorevi tarafindan cagrilir (ISR degil, ancak ISR kurallari gecerli).
// Uzun islemler yapma. syncSemaphore ile meshTask'i uyandiriyoruz.
void OnDataRecv(const uint8_t* mac, const uint8_t* incomingData, int len) {
    if (len != sizeof(MeshPacket)) return;
    int rssi = latest_rssi;
    MeshPacket pkt; memcpy(&pkt, incomingData, sizeof(MeshPacket));

    // ── SYNC ─────────────────────────────────────────────────────────
    if (pkt.type == TYPE_SYNC) {
        // Her halukarda komsu tablosunu guncel tutalim ki loglarda gorebilelim:
        if (strcmp(pkt.sender, "GW") == 0) {
            if (!hasGatewayMac) {
                memcpy(gateway_mac, mac, 6); hasGatewayMac = true;
                addOrUpdateNeighbor("GW", mac, rssi, 0, "GW");
                Serial.printf("\n[SECURE] Gateway MAC: %02X:%02X:%02X:%02X:%02X:%02X\n",
                              mac[0],mac[1],mac[2],mac[3],mac[4],mac[5]);
            } else {
                for (int i=0;i<neighborCount;i++) if(strcmp(neighbors[i].nodeId,"GW")==0){
                    neighbors[i].linkRssi=(int)(0.85f*neighbors[i].linkRssi+0.15f*rssi);
                    neighbors[i].lastSeen=millis(); break;
                }
            }
        } else {
            addOrUpdateNeighbor(pkt.sender, pkt.senderMac, rssi, pkt.gwRssi, pkt.path);
        }

        // -70 dBm altindaysa baglanma/senkron olma!
        if (rssi < MIN_LINK_RSSI) {
            return;
        }

        lastSyncTime     = millis();
        missedSyncCount  = 0;
        relayedThisCycle = false;

        if (strstr(pkt.payload, "A1") != NULL) isAwayMode = true;
        else if (strstr(pkt.payload, "A0") != NULL) isAwayMode = false;

        if (strcmp(pkt.sender, "GW") == 0) {
            myGwRssi = rssi;
            lastDirectGwSyncTime = millis();
            relaySyncBroadcast(mac, rssi, pkt.ttl);
        } else {
            if (!hasGatewayMac) {
                memcpy(gateway_mac, pkt.destMac, 6); hasGatewayMac = true;
                Serial.printf("\n[RELAY-SYNC] GW MAC ogrenildi! Relay: %s\n", pkt.sender);
            }
            
            if (hasGatewayMac && (millis() - lastDirectGwSyncTime < 25000)) {
                // Gateway'i zaten duyuyorum, komşunun SYNC mesajini routing icin IGNORELA.
                // (myGwRssi degerini EZME).
            } else {
                // GW'yi duymuyorum (veya cok uzun zaman oldu), sadece en guclu komsuya cevap ver
                int cand = min((int)pkt.gwRssi, rssi) - 5; // Hop cezası
                if (cand > myGwRssi || myGwRssi == -999 || (millis() - lastRelaySyncTime > 25000)) {
                    myGwRssi = cand;
                    lastRelaySyncTime = millis();
                }
            }
        }
        if (!isSynced) {
            isSynced = true;
            discoveryAttemptCount = 0;
            uint8_t primary;
            wifi_second_chan_t second;
            esp_wifi_get_channel(&primary, &second);
            knownChannel = primary;
            Serial.printf("\n[SYNC] (%s uzerinden) | myGwRssi=%d | Kanal Kaydedildi: %d\n", pkt.sender, myGwRssi, knownChannel);
        }

        // meshTask'i uyandır — syncSemaphore ver
        BaseType_t xHigherPriorityTaskWoken = pdFALSE;
        xSemaphoreGiveFromISR(syncSemaphore, &xHigherPriorityTaskWoken);
        portYIELD_FROM_ISR(xHigherPriorityTaskWoken);
        return;
    }

    // ── DISCOVERY ────────────────────────────────────────────────────
    if (pkt.type == TYPE_DISCOVERY) {
        addOrUpdateNeighbor(pkt.sender, pkt.senderMac, rssi, pkt.gwRssi, pkt.path);
        Serial.printf("<- [DISC] %-8s Link:%d GW:%d\n", pkt.sender, rssi, pkt.gwRssi);
        if (isSynced && hasGatewayMac) relaySyncUnicast(pkt.senderMac);
        return;
    }

    // ── HEARTBEAT RELAY (bana adreslenenmis) ─────────────────────────
    if (pkt.type == TYPE_HEARTBEAT && strcmp(pkt.receiver, myId) == 0) {
        addOrUpdateNeighbor(pkt.sender, pkt.senderMac, rssi, pkt.gwRssi, pkt.path);
        
        bool allowRelay = false;
        unsigned long now = millis();
        
        for(int i=0; i<neighborCount; i++) {
            if(strcmp(neighbors[i].nodeId, pkt.sender) == 0) {
                if(now - neighbors[i].lastRelayedHB > 5000) {
                    neighbors[i].lastRelayedHB = now;
                    allowRelay = true;
                }
                break;
            }
        }
        

        if (allowRelay && hasGatewayMac && pkt.ttl > 0) {
            char np[60];
            snprintf(np, sizeof(np), "%s>%s", pkt.path, myId);
            strncpy(pkt.path, np, sizeof(pkt.path)-1);
            pkt.ttl--;
            
            const uint8_t* targetMac = NULL;
            strcpy(pkt.receiver, "");
            
            if (currentHopId[0] != '\0') {
                for(int i=0; i<neighborCount; i++){
                    if(strcmp(neighbors[i].nodeId, currentHopId)==0){
                        targetMac = neighbors[i].macAddr;
                        strcpy(pkt.receiver, currentHopId);
                        break;
                    }
                }
            } else if (millis() - lastDirectGwSyncTime < 25000) {
                targetMac = gateway_mac;
                strcpy(pkt.receiver, "GW");
            }
            
            if (targetMac != NULL) {
                for(int k=0; k<3; k++) {
                    esp_now_send(targetMac, (uint8_t*)&pkt, sizeof(MeshPacket));
                    vTaskDelay(pdMS_TO_TICKS(15));
                }
                Serial.printf("[RELAY-HB] %s -> %s | %s\n", pkt.sender, pkt.receiver, pkt.path);
            } else {
                Serial.printf("[RELAY-HB-DROP] Rota yok veya GW baglantisi stale, paket atildi! Kaynak: %s\n", pkt.sender);
            }
        }
        return;
    }

    // ── HEARTBEAT (benim icin degil, sadece komsuyu guncelle) ────────
    if (pkt.type == TYPE_HEARTBEAT) {
        addOrUpdateNeighbor(pkt.sender, pkt.senderMac, rssi, pkt.gwRssi, pkt.path);
        return;
    }

    // ── ANOMALY RELAY ─────────────────────────────────────────────────
    if (pkt.type == TYPE_EMERGENCY_ANOMALY && strcmp(pkt.receiver, myId) == 0) {
        if (pkt.ttl <= 0) return;
        char np[60];
        snprintf(np, sizeof(np), "%s>%s>GW", pkt.path, myId);
        strncpy(pkt.path, np, sizeof(pkt.path)-1);
        pkt.ttl--;
        strcpy(pkt.receiver, "GW");
        if (hasGatewayMac) {
            for(int k=0; k<3; k++) {
                esp_now_send(gateway_mac, (uint8_t*)&pkt, sizeof(MeshPacket));
                vTaskDelay(pdMS_TO_TICKS(15));
            }
            Serial.printf("[RELAY-ANOM] %s -> GW | %s\n", pkt.sender, pkt.path);
        }
        return;
    }
}

void OnDataSent(const uint8_t* mac_addr, esp_now_send_status_t status) {
    const char* targetName = "Bilinmeyen";
    if (hasGatewayMac && memcmp(mac_addr, gateway_mac, 6) == 0) {
        targetName = "GW";
    } else {
        for (int i = 0; i < neighborCount; i++) {
            if (memcmp(neighbors[i].macAddr, mac_addr, 6) == 0) {
                targetName = neighbors[i].nodeId;
                break;
            }
        }
    }
    if (status == ESP_NOW_SEND_SUCCESS) {
        Serial.printf("[TX] -> %s: BASARILI\n", targetName);
    } else {
        Serial.printf("[TX] -> %s: HATA! (Paket ulastirilamadi)\n", targetName);
    }
}

// ==================== ROUTING ====================
int selectNextHop() {
    int   bestIdx   = -1;
    float bestScore = -999.0f;
    int   curIdx    = -1;

    // Eger Gateway'e baglantim yeterince iyiyse hic hop arama, dogrudan GW kullan
    if (hasGatewayMac && myGwRssi >= MIN_LINK_RSSI && (millis() - lastDirectGwSyncTime < 25000)) {
        if (currentHopId[0] != '\0') {
            Serial.printf("[ROTA] Sinyal iyi (GW'yi duyuyorum), relay IPTAL -> GW\n");
            currentHopId[0] = '\0';
        }
        return -1; // -1 donerse main code GW'ye gonderir
    }

    for (int i = 0; i < neighborCount; i++) {
        if (strcmp(neighbors[i].nodeId, "GW") == 0) continue;
        if (millis() - neighbors[i].lastSeen > 25000UL) continue;
        if (neighbors[i].linkRssi < MIN_LINK_RSSI) continue;
        if (neighbors[i].gwRssi <= myGwRssi + LOOP_GUARD_DB) continue;

        // SPLIT-HORIZON KONTROLU (LOOP ONLEME)
        // Eger bu komsunun rotasinda benim adim geciyorsa onu kesinlikle secmem!
        if (neighbors[i].path[0] != '\0' && strstr(neighbors[i].path, myId) != NULL) {
            // Serial.printf("[LOOP-ONLENDI] %s -> %s\n", myId, neighbors[i].nodeId);
            continue;
        }

        if (strcmp(neighbors[i].nodeId, currentHopId) == 0) curIdx = i;

        // GW'ye yakinliga daha buyuk agirlik ver
        float score = 0.7f * neighbors[i].gwRssi + 0.3f * neighbors[i].linkRssi;
        if (score > bestScore) { bestScore = score; bestIdx = i; }
    }

    if (curIdx >= 0 && bestIdx >= 0 && bestIdx != curIdx) {
        if (bestScore < currentHopScore + HOP_HYSTERESIS_DB) return curIdx;
    }

    if (bestIdx >= 0) {
        if (strcmp(neighbors[bestIdx].nodeId, currentHopId) != 0) {
            Serial.printf("[ROTA] %s -> %s (skor:%.1f)\n",
                          currentHopId[0] ? currentHopId : "GW",
                          neighbors[bestIdx].nodeId, bestScore);
        }
        strncpy(currentHopId, neighbors[bestIdx].nodeId, sizeof(currentHopId)-1);
        currentHopScore = bestScore;
    } else {
        if (currentHopId[0] != '\0') {
            Serial.printf("[ROTA] %s -> GW (relay yok)\n", currentHopId);
        }
        currentHopId[0] = '\0';
        currentHopScore  = -999.0f;
    }
    return bestIdx;
}

// ==================== Z-SCORE ====================
void addToWindows(float t, float h, int p, float d) {
    tempWindow[windowIdx] = t;
    humWindow[windowIdx]  = h;
    pirWindow[windowIdx]  = p;
    diffWindow[windowIdx] = d;
    windowIdx = (windowIdx + 1) % WINDOW_SIZE;
    if (windowCount < WINDOW_SIZE) windowCount++;
}

float calcTempZScore(float t) {
    if (windowCount < 5) return 0.0f;
    float s=0,sq=0; int n=min(windowCount,WINDOW_SIZE);
    for (int i=0;i<n;i++){s+=tempWindow[i];sq+=tempWindow[i]*tempWindow[i];}
    float m=s/n,var=(sq/n)-(m*m);
    return fabs(t-m)/((var>0)?sqrt(var):0.001f);
}

char encodeDelta(int d) {
    if (d < 0) d = 0;
    if (d > 61) d = 61;
    if (d < 26) return 'A' + d;
    if (d < 52) return 'a' + (d - 26);
    return '0' + (d - 52);
}

String buildCompressedPayload(const char* prefix) {
    int n = min(windowCount, WINDOW_SIZE);
    if (n == 0) return String(prefix) + "|NO_DATA";
    float minT = 999.0, minH = 999.0;
    for (int i=0; i<n; i++) {
        if (tempWindow[i] < minT) minT = tempWindow[i];
        if (humWindow[i] < minH) minH = humWindow[i];
    }
    int baseT = (int)(minT * 10);
    int baseH = (int)(minH * 10);
    String sT = "", sH = "", sP = "";
    int start = (windowCount == WINDOW_SIZE) ? windowIdx : 0;
    for (int i=0; i<n; i++) {
        int idx = (start + i) % WINDOW_SIZE;
        sT += encodeDelta((int)(tempWindow[idx]*10) - baseT);
        sH += encodeDelta((int)(humWindow[idx]*10) - baseH);
        sP += pirWindow[idx] ? "1" : "0";
    }
    char buf[120];
    snprintf(buf, sizeof(buf), "%s|%d,%d|%s|%s|%s", prefix, baseT, baseH, sT.c_str(), sH.c_str(), sP.c_str());
    return String(buf);
}

String buildHeartbeatPayload() {
    String s = "ALIVE|N:";
    int n = min(neighborCount, 4);
    for (int i=0;i<n;i++) { if (i>0) s += ";"; s += String(neighbors[i].nodeId); }
    return s;
}

// ==================== VERI GONDERIMI ====================
// meshTask icinden cagrilir. windowMutex caller tarafindan alinmis olmali.
void processSensorsAndSend() {
    if (totalCount==0) {
        MeshPacket h; memset(&h,0,sizeof(h));
        strcpy(h.sender,myId); strcpy(h.receiver,"FFFF");
        memcpy(h.senderMac,myMac,6);
        h.type=TYPE_DISCOVERY; h.ttl=1; h.gwRssi=(int16_t)myGwRssi;
        strcpy(h.path,myId); strcpy(h.payload,"HELLO_MESH");
        esp_now_send(broadcastAddress,(uint8_t*)&h,sizeof(MeshPacket));
        Serial.println("-> [KESIF] Discovery atildi!");
    }
    totalCount++;

    bool isAnomaly = anomaliTetikte;
    bool isPir     = pirTetikte;
    anomaliTetikte = false;
    pirTetikte     = false;

    MeshPacket pkt; memset(&pkt,0,sizeof(pkt));
    strcpy(pkt.sender,myId);
    memcpy(pkt.senderMac,myMac,6);
    if (hasGatewayMac) memcpy(pkt.destMac,gateway_mac,6);
    pkt.gwRssi=(int16_t)myGwRssi;
    pkt.ttl=MAX_HOPS;
    snprintf(pkt.path,sizeof(pkt.path),"%s",myId);

    if (isAnomaly) {
        pkt.type=TYPE_EMERGENCY_ANOMALY;
        String b = buildCompressedPayload(isPir ? "P_ALARM" : "Z_ALARM");
        strncpy(pkt.payload, b.c_str(), sizeof(pkt.payload)-1);
        Serial.printf("[!!! ANOMALI GONDERILIYOR !!!] GW=%d | Veri: %s\n", myGwRssi, b.c_str());
    } else {
        pkt.type=TYPE_HEARTBEAT;
        float curT = windowCount > 0 ? tempWindow[(windowIdx-1+WINDOW_SIZE)%WINDOW_SIZE] : 0.0;
        float curH = windowCount > 0 ? humWindow[(windowIdx-1+WINDOW_SIZE)%WINDOW_SIZE] : 0.0;
        String hbp = buildHeartbeatPayload();
        snprintf(pkt.payload, sizeof(pkt.payload)-1, "T:%.1f|H:%.1f|%s", curT, curH, hbp.c_str());
        Serial.printf("[Normal] T:%.1fC H:%.1f%% GW=%d\n", curT, curH, myGwRssi);
    }

    int hopIdx = selectNextHop();
    const uint8_t* targetMac = broadcastAddress;
    
    if (hopIdx >= 0) {
        NeighborNode& hop = neighbors[hopIdx];
        strcpy(pkt.receiver, hop.nodeId);
        targetMac = hop.macAddr;
        Serial.printf("  -> %s -> %s -> GW\n", myId, hop.nodeId);
    } else if (hasGatewayMac && (millis() - lastDirectGwSyncTime < 25000)) {
        char fp[60]; snprintf(fp,sizeof(fp),"%s>GW",pkt.path);
        strncpy(pkt.path,fp,sizeof(pkt.path)-1);
        strcpy(pkt.receiver,"GW");
        targetMac = gateway_mac;
        Serial.println("  -> Direkt GW");
    } else {
        strcpy(pkt.receiver,"FFFF");
        targetMac = broadcastAddress;
        Serial.println("  -> Broadcast Fallback");
    }
    
    for(int k=0; k<3; k++) {
        esp_now_send(targetMac, (uint8_t*)&pkt, sizeof(MeshPacket));
        vTaskDelay(pdMS_TO_TICKS(15));
    }
}

// ==================== SENSOR GOREVI (Core 0) ====================
// Her zaman calisir — anten kapali olsa bile DHT11 ve PIR olcumu yapar.
// Olcum sonuclarini windowMutex ile korunan pencere dizilerine yazar.
void sensorTask(void* param) {
    unsigned long lastDhtMs = 0;
    Serial.println("[sensorTask] Basladi (Core 0).");

    while (true) {
        unsigned long now = millis();

        // DHT11'i her 1 saniyede bir oku
        if (now - lastDhtMs >= 1000) {
            lastDhtMs = now;
            TempAndHumidity r = dht.getTempAndHumidity();
            int pirVal = isAwayMode ? digitalRead(PIRPIN) : 0;

            if (!isnan(r.temperature) && !isnan(r.humidity)) {
                // Pencere dizilerine mutex ile yaz
                if (xSemaphoreTake(windowMutex, pdMS_TO_TICKS(200)) == pdTRUE) {
                    float diff = (prevValue > -900) ? (r.temperature - prevValue) : 0;
                    prevValue = r.temperature;

                    // Z-Score anomali kontrolu (pencere okuma da mutex altinda)
                    float zScore = calcTempZScore(r.temperature);
                    if (zScore > 2.5f && windowCount >= 5) {
                        anomaliTetikte = true;
                        Serial.printf("[!!! ISI ANOMALISI !!!] Z=%.2f\n", zScore);
                    }

                    addToWindows(r.temperature, r.humidity, pirVal, diff);
                    xSemaphoreGive(windowMutex);
                }
                // Serial.printf("[SENSOR] T:%.1fC H:%.1f%% PIR:%d (Window:%d/%d)\n", r.temperature, r.humidity, pirVal, windowCount, WINDOW_SIZE); // Pil tasarrufu icin kaldirildi
            } else {
                Serial.println("[SENSOR] DHT Okuma Hatasi!");
            }

            // ── RSSI Logu (her 1 saniyede bir) ───────────────────────────
            Serial.printf("\n┌─ RSSI ──────────────────────────────────────────────────┐\n");
            Serial.printf("│ myGwRssi : %4d dBm | isSynced: %s               │\n",
                          myGwRssi, isSynced ? "EVET" : "HAYIR");
            Serial.printf("├─────────────────────────────────────────────────────────┤\n");
            if (neighborCount == 0) {
                Serial.printf("│ Komsu yok.                                              │\n");
            } else {
                for (int i = 0; i < neighborCount; i++) {
                    long ageSec = (millis() - neighbors[i].lastSeen) / 1000;
                    bool stale  = ageSec > 25;
                    bool weak   = neighbors[i].linkRssi < MIN_LINK_RSSI;
                    const char* statusStr = stale ? "[STALE] " : (weak ? "[ZAYIF] " : "[OK]    ");
                    Serial.printf("│ %-8s | Link: %4d dBm | GW: %4d dBm | %3lds %s │\n",
                                  neighbors[i].nodeId,
                                  neighbors[i].linkRssi,
                                  neighbors[i].gwRssi,
                                  ageSec,
                                  statusStr);
                }
            }
            Serial.printf("└─────────────────────────────────────────────────────────┘\n");
        }

        // PIR interrupt flag kontrolu (50ms'de bir)
        if (pirTriggered) {
            pirTriggered = false;
            if (isAwayMode) {
                anomaliTetikte = true;
                pirTetikte     = true;
                Serial.println("[PIR] HAREKET ALGILANDI!");
            }
        }

        vTaskDelay(pdMS_TO_TICKS(50)); // 50ms — PIR'a hizli yanit
    }
}

// ==================== MESH GOREVI (Core 1) ====================
// SYNC bekleme, paket gonderme, WiFi (anten) acma/kapama.
// "Uyku" suresi boyunca sadece anten kapali — sensorTask calismeye devam eder.
void meshTask(void* param) {
    int scanCh = 1;
    Serial.println("[meshTask] Basladi (Core 1).");

    while (true) {

        // ── SENKRON DEGILSE: Kanal Tarama ────────────────────────────
        if (!isSynced) {
            uint8_t cCh;
            wifi_second_chan_t sCh;
            esp_wifi_get_channel(&cCh, &sCh);
            if (cCh != scanCh) {
                esp_wifi_set_channel(scanCh, WIFI_SECOND_CHAN_NONE);
                Serial.printf("[SCAN] Kanal degisti -> %d\n", scanCh);
            }

            MeshPacket h; memset(&h, 0, sizeof(h));
            strcpy(h.sender, myId); strcpy(h.receiver, "FFFF");
            memcpy(h.senderMac, myMac, 6);
            h.type = TYPE_DISCOVERY; h.ttl = 1; h.gwRssi = (int16_t)myGwRssi;
            strcpy(h.path, myId); strcpy(h.payload, "TELSIZ");
            esp_now_send(broadcastAddress, (uint8_t*)&h, sizeof(MeshPacket));

            // Tarama periyodunda ben de DISCOVERY atiyorum ki Gateway veya birisi beni duyarsa bana donus yapsin
            // Uyuyan cihazlarin (hop nodelar) 1-1.5 saniyelik uyaniklik penceresini
            // kacirmamak icin tum spektrumu 1 saniyede gezmeliyiz. (13 kanal * 76ms = ~988ms)
            if (xSemaphoreTake(syncSemaphore, pdMS_TO_TICKS(76)) != pdTRUE) {
                if (knownChannel > 0 && discoveryAttemptCount < 300) {
                    scanCh = knownChannel;
                    discoveryAttemptCount++;
                } else {
                    scanCh = (scanCh % 13) + 1;
                    discoveryAttemptCount++;
                }
            }
            continue;
        }

        // ── SENKRON: SYNC Bekleme ─────────────────────────────────────
        if (xSemaphoreTake(syncSemaphore, pdMS_TO_TICKS((uint32_t)CYCLE_PERIOD_MS * 3)) == pdTRUE) {
            missedSyncCount = 0;
            lastProcessTime = millis();

            // Pencere verilerini mutex ile oku, paketi gonder
            if (xSemaphoreTake(windowMutex, pdMS_TO_TICKS(500)) == pdTRUE) {
                processSensorsAndSend();
                xSemaphoreGive(windowMutex);
            } else {
                Serial.println("[UYARI] windowMutex zaman asimi — gonderim atlandi!");
            }

            // Relay penceresi: AWAKE_DURATION_MS boyunca uyanik kal
            // Bu sure icinde diger nodelardan relay paketleri gelebilir
            vTaskDelay(pdMS_TO_TICKS(AWAKE_DURATION_MS));

            // ── ANTEN KAPAT ─────────────────────────────────────────
            uint8_t currentCh;
            wifi_second_chan_t secCh;
            esp_wifi_get_channel(&currentCh, &secCh);

            esp_now_deinit();
            WiFi.mode(WIFI_OFF);
            Serial.printf("[ANTEN KAPALI] Kanal %d kaydedildi.\n", currentCh);
            // sensorTask bu sure boyunca Core 0'da olcum yapmaya devam eder!

            // Dinamik Uyku Süresi Hesaplama (SYNC Drift Onleyici)
            unsigned long now = millis();
            long timeSinceSync = now - lastSyncTime; // En son SYNC geldiginden beri gecen sure
            long timeLeftInCycle = CYCLE_PERIOD_MS - timeSinceSync;
            long preWakeMs = 800; // GW'nin atisindan 800 ms once uyan
            long sleepMs = timeLeftInCycle - preWakeMs;

            if (sleepMs > 100 && sleepMs < CYCLE_PERIOD_MS) {
                Serial.printf("[UYKU] Dinamik Sure: %ld ms\n", sleepMs);
                vTaskDelay(pdMS_TO_TICKS((uint32_t)sleepMs));
            } else {
                Serial.printf("[UYARI] Uyku suresi mantiksiz (%ld ms), anten acik kalacak!\n", sleepMs);
            }

            // ── ANTEN AÇ ─────────────────────────────────────────────
            Serial.println("[ANTEN ACIK] Sync bekleniyor...");
            WiFi.mode(WIFI_STA);
            esp_wifi_set_channel(currentCh, WIFI_SECOND_CHAN_NONE);

            if (esp_now_init() == ESP_OK) {
                esp_now_register_recv_cb(OnDataRecv);
                esp_now_register_send_cb(OnDataSent);
                esp_now_add_peer(&peerInfo); // Broadcast peer
                // Uyku oncesi silinen komsu peer listesini geri yukle
                for (int i = 0; i < neighborCount; i++) {
                    esp_now_peer_info_t p;
                    memset(&p, 0, sizeof(p));
                    memcpy(p.peer_addr, neighbors[i].macAddr, 6);
                    p.channel = 0; p.encrypt = false;
                    if (!esp_now_is_peer_exist(neighbors[i].macAddr)) esp_now_add_peer(&p);
                }
                esp_wifi_set_promiscuous_rx_cb(&promisc_cb);
                esp_wifi_set_promiscuous(true);
            } else {
                Serial.println("[HATA] ESP-NOW uyanirken baslatilamadi!");
            }

        } else {
            // SYNC zaman asimi
            missedSyncCount++;
            Serial.printf("[UYARI] SYNC kacirildi! (%d/3)\n", missedSyncCount);

            if (missedSyncCount >= 3) {
                Serial.println("[KRITIK] Baglanti kesildi! Yeniden tarama baslatiyor...");
                isSynced         = false;
                hasGatewayMac    = false;
                neighborCount    = 0;
                missedSyncCount  = 0;
                myGwRssi         = -127;
                lastProcessTime  = 0;
                relayedThisCycle = false;
                currentHopId[0]  = '\0';
                currentHopScore  = -999.0f;
                scanCh           = 1;

                // WiFi kapali kalmiş olabilir, geri ac
                if (WiFi.getMode() == WIFI_OFF) {
                    WiFi.mode(WIFI_STA);
                    esp_wifi_set_channel(1, WIFI_SECOND_CHAN_NONE);
                    if (esp_now_init() == ESP_OK) {
                        esp_now_register_recv_cb(OnDataRecv);
                        esp_now_register_send_cb(OnDataSent);
                        esp_now_add_peer(&peerInfo);
                        esp_wifi_set_promiscuous_rx_cb(&promisc_cb);
                        esp_wifi_set_promiscuous(true);
                    }
                }
            }
        }
    }
}

// ==================== SETUP ====================
void setup() {
    Serial.begin(115200);
    randomSeed(analogRead(0));
    WiFi.macAddress(myMac);
    sprintf(myId,"N_%02X%02X",myMac[4],myMac[5]);

    Serial.println("\n\u2554\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2557");
    Serial.println(  "\u2551           CIHAZ MAC ADRESI           \u2551");
    Serial.printf( "\u2551  >> %02X:%02X:%02X:%02X:%02X:%02X <<        \u2551\n",
                  myMac[0],myMac[1],myMac[2],myMac[3],myMac[4],myMac[5]);
    Serial.printf( "\u2551  ID : %-8s                    \u2551\n", myId);
    Serial.println("\u2551  Rol: WROOM SLAVE (FreeRTOS)         \u2551");
    Serial.println("\u255a\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u2550\u255d\n");

    dht.setup(DHTPIN, DHTesp::DHT11);
    pinMode(PIRPIN, INPUT);
    attachInterrupt(digitalPinToInterrupt(PIRPIN), pir_isr, RISING);

    WiFi.mode(WIFI_STA);
    esp_wifi_set_channel(WIFI_CHANNEL, WIFI_SECOND_CHAN_NONE);
    esp_wifi_set_promiscuous_rx_cb(&promisc_cb);
    esp_wifi_set_promiscuous(true);

    if (esp_now_init() != ESP_OK) { Serial.println("ESP-NOW HATA!"); return; }
    esp_now_register_send_cb(OnDataSent);
    esp_now_register_recv_cb(OnDataRecv);
    memcpy(peerInfo.peer_addr, broadcastAddress, 6);
    peerInfo.channel = 0; peerInfo.encrypt = false;
    esp_now_add_peer(&peerInfo);

    // DHT11 isinma suresi
    Serial.println("[DHT] Sensor isinma bekleniyor...");
    delay(1500);

    // ── RTOS Nesnelerini Olustur ─────────────────────────────────────
    syncSemaphore = xSemaphoreCreateBinary();
    windowMutex   = xSemaphoreCreateMutex();

    if (!syncSemaphore || !windowMutex) {
        Serial.println("[HATA] RTOS nesneleri olusturulamadi! Reset...");
        ESP.restart();
        return;
    }

    // ── RTOS Gorevlerini Baslat ──────────────────────────────────────
    // sensorTask: Core 0, Priority 1 — her zaman calisir
    xTaskCreatePinnedToCore(sensorTask, "SensorTask", 4096, NULL, 1, &sensorTaskHandle, 0);
    // meshTask  : Core 1, Priority 2 — ESP-NOW + SYNC kritik islemler
    xTaskCreatePinnedToCore(meshTask,   "MeshTask",   8192, NULL, 2, &meshTaskHandle,   1);

    Serial.println("[HAZIR] FreeRTOS gorevleri basladi.");
}

// ==================== ANA DONGU ====================
// loop() artik kullanilmiyor.
// Tum isler sensorTask (Core 0) ve meshTask (Core 1)'e devredildi.
void loop() {
    vTaskDelay(portMAX_DELAY);
}
