#include <PZEM004Tv30.h>
#include <SoftwareSerial.h> 
#include <TimeLib.h>      

// --- Configuration ---
#if defined(ESP32)
    #error "SoftwareSerial is not supported on ESP32. Use HardwareSerial."
#else
    #define PZEM_RX_PIN 12 // Connect this to PZEM TX pin
    #define PZEM_TX_PIN 13 // Connect this to PZEM RX pin
    SoftwareSerial pzemSerial(PZEM_RX_PIN, PZEM_TX_PIN);
    PZEM004Tv30 pzem(pzemSerial);
#endif

#define RELAY_PIN 7 

// Sampling interval (milliseconds)
const unsigned long SAMPLE_INTERVAL = 1000; // Send data every 1 second when ON
unsigned long lastSampleTime = 0;

// State variable
bool bulbOn = false; 

// --- Function Prototypes ---
void turnBulbOn();
void turnBulbOff();
void processSerialCommands();
void logMeasurements();
String getTimestamp();
time_t compileTime();
void printHeader();

// --- Setup ---
void setup() {
    Serial.begin(115200);
    delay(1500);

    Serial.println("PZEM004T Logger with Relay Control Initializing (Active-HIGH on D7)...");

    pinMode(RELAY_PIN, OUTPUT);
    
    digitalWrite(RELAY_PIN, LOW);
    bulbOn = false;
    Serial.println("Relay Pin (D7) configured. Initial state: OFF");

    // Test PZEM communication 
    uint8_t address = pzem.readAddress();
    if (address == 0xFF || address == 0x00) {
         Serial.println("Error: Unable to communicate with PZEM module. Check wiring/address.");
    } else {
        Serial.print("PZEM module detected at address: 0x");
        Serial.println(address, HEX);
    }

    setTime(compileTime());
    Serial.println("Initial time set from compile time.");
    Serial.println("Ready. Waiting for commands ('O' = ON, 'F' = OFF)...");
    printHeader();
}

// --- Main Loop --- 
void loop() {
    processSerialCommands();
    if (bulbOn && (millis() - lastSampleTime >= SAMPLE_INTERVAL)) {
        lastSampleTime = millis();
        logMeasurements();
    }
    delay(50);
}

// --- Functions ---

void printHeader() { 
    Serial.println("# Timestamp,Address,Voltage(V),Current(A),Power(W),Energy(kWh),Frequency(Hz),PF");
}

void turnBulbOn() {
    if (!bulbOn) {
        digitalWrite(RELAY_PIN, HIGH);
        bulbOn = true;
        lastSampleTime = millis();
        Serial.println("# Status: Turned ON");
    } else {
        Serial.println("# Status: Already ON");
    }
}

void turnBulbOff() {
    if (bulbOn) {
        digitalWrite(RELAY_PIN, LOW);
        bulbOn = false;
        Serial.println("# Status: Turned OFF");
    } else {
        Serial.println("# Status: Already OFF");
    }
}

void processSerialCommands() { 
    if (Serial.available() > 0) {
        char command = Serial.read();
        if (command == '\n' || command == '\r') { return; }
        Serial.print("# Received command: "); Serial.println(command);
        if (command == 'O' || command == 'o') { turnBulbOn(); }
        else if (command == 'F' || command == 'f') { turnBulbOff(); }
        else { Serial.print("# Error: Unknown command '"); Serial.print(command); Serial.println("'"); Serial.println("# Available commands: O (ON), F (OFF)"); }
        while(Serial.available() > 0) { Serial.read(); }
    }
}

void logMeasurements() { 
    String timestamp = getTimestamp();
    uint8_t address = pzem.readAddress();
    float voltage = pzem.voltage();
    float current = pzem.current();
    float power = pzem.power();
    float energy = pzem.energy();
    float frequency = pzem.frequency();
    float pf = pzem.pf();
    if (isnan(voltage) || isnan(current) || isnan(power) || isnan(energy) || isnan(frequency) || isnan(pf) || address == 0xFF || address == 0x00) {
        Serial.println("# Error: Failed to read valid data from PZEM module.");
        return;
    }
    String dataString = timestamp; dataString += ",";
    char addrStr[4]; sprintf(addrStr, "%02X", address); dataString += addrStr; dataString += ",";
    dataString += String(voltage, 2); dataString += ",";
    dataString += String(current, 3); dataString += ",";
    dataString += String(power, 2); dataString += ",";
    dataString += String(energy, 5); dataString += ",";
    dataString += String(frequency, 1); dataString += ",";
    dataString += String(pf, 2);
    Serial.println(dataString);
}

String getTimestamp() { 
    char buffer[20];
    sprintf(buffer, "%04d-%02d-%02d %02d:%02d:%02d", year(), month(), day(), hour(), minute(), second());
    return String(buffer);
}

time_t compileTime() { 
    const char* dateStr = __DATE__; const char* timeStr = __TIME__;
    char monthStr[4]; int dayOfMonth, yearNum; sscanf(dateStr, "%s %d %d", monthStr, &dayOfMonth, &yearNum);
    int hourNum, minNum, secNum; sscanf(timeStr, "%d:%d:%d", &hourNum, &minNum, &secNum);
    const char* months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    int monthNum = 0; for (int i = 0; i < 12; ++i) { if (strcmp(monthStr, months[i]) == 0) { monthNum = i + 1; break; } }
    tmElements_t tm; tm.Year = CalendarYrToTm(yearNum); tm.Month = monthNum; tm.Day = dayOfMonth;
    tm.Hour = hourNum; tm.Minute = minNum; tm.Second = secNum;
    return makeTime(tm);
}