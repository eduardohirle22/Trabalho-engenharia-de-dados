"""
UrbanFlow — Gerador de Dados Simulados
Prova de Conceito: simula eventos de GPS de ônibus e os publica no Kafka.

Uso:
    pip install kafka-python faker
    python simulate_gps_producer.py

Pré-requisito: Kafka rodando em localhost:9092 (via docker-compose)
"""

import json
import time
import random
import math
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import List

try:
    from kafka import KafkaProducer
    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    print("⚠️  kafka-python não instalado. Rodando em modo DRY-RUN (apenas imprime no console).")


# ─────────────────────────────────────────────
# Configuração da simulação
# ─────────────────────────────────────────────

KAFKA_BROKER = "localhost:9092"
TOPIC_GPS = "gps-onibus"

# Linhas de ônibus simuladas
LINHAS = [f"L{str(i).zfill(3)}" for i in range(1, 21)]  # 20 linhas

# Centro da cidade (simulado — coordenadas fictícias baseadas em São Paulo)
LAT_CENTER = -23.5505
LON_CENTER = -46.6333
RADIUS_KM = 15.0  # raio da cidade em km

# Horários de pico (maior frequência de passageiros)
PEAK_HOURS = [(7, 9), (12, 13), (17, 19)]


@dataclass
class GPSEvent:
    vehicle_id: str
    line_id: str
    lat: float
    lon: float
    speed_kmh: float
    occupancy_pct: int
    status: str  # "on_route", "at_stop", "delayed"
    timestamp: str


def is_peak_hour(hour: int) -> bool:
    """Verifica se é horário de pico."""
    return any(start <= hour < end for start, end in PEAK_HOURS)


def random_coord_near_center(radius_km: float = RADIUS_KM) -> tuple:
    """Gera coordenadas aleatórias dentro de um raio do centro."""
    # Conversão aproximada: 1 grau ≈ 111 km
    r = radius_km / 111.0
    angle = random.uniform(0, 2 * math.pi)
    distance = random.uniform(0, r)
    lat = LAT_CENTER + distance * math.cos(angle)
    lon = LON_CENTER + distance * math.sin(angle)
    return round(lat, 6), round(lon, 6)


def simulate_speed(status: str, is_peak: bool) -> float:
    """Simula velocidade realista baseada no status e horário."""
    if status == "at_stop":
        return 0.0
    elif status == "delayed" or is_peak:
        return round(random.uniform(5, 25), 1)
    else:
        return round(random.uniform(20, 55), 1)


def simulate_occupancy(is_peak: bool) -> int:
    """Simula taxa de ocupação do ônibus."""
    if is_peak:
        return random.randint(60, 100)
    else:
        return random.randint(10, 60)


def generate_vehicle_fleet(n_vehicles: int = 50) -> List[dict]:
    """Gera frota inicial com posições aleatórias."""
    fleet = []
    for i in range(1, n_vehicles + 1):
        lat, lon = random_coord_near_center()
        fleet.append({
            "vehicle_id": f"BUS-{str(i).zfill(4)}",
            "line_id": random.choice(LINHAS),
            "lat": lat,
            "lon": lon,
        })
    return fleet


def move_vehicle(vehicle: dict, is_peak: bool) -> dict:
    """Simula o movimento de um veículo."""
    # Pequeno deslocamento aleatório (simula movimento na rota)
    delta = 0.002 if not is_peak else 0.001  # menor deslocamento no pico
    vehicle["lat"] += random.uniform(-delta, delta)
    vehicle["lon"] += random.uniform(-delta, delta)
    return vehicle


def create_gps_event(vehicle: dict, is_peak: bool) -> GPSEvent:
    """Cria um evento GPS para um veículo."""
    status = random.choices(
        ["on_route", "at_stop", "delayed"],
        weights=[0.7, 0.2, 0.1] if not is_peak else [0.5, 0.2, 0.3]
    )[0]

    return GPSEvent(
        vehicle_id=vehicle["vehicle_id"],
        line_id=vehicle["line_id"],
        lat=round(vehicle["lat"], 6),
        lon=round(vehicle["lon"], 6),
        speed_kmh=simulate_speed(status, is_peak),
        occupancy_pct=simulate_occupancy(is_peak),
        status=status,
        timestamp=datetime.now(timezone.utc).isoformat()
    )


def main():
    print("🚌 UrbanFlow — Simulador de GPS de Ônibus")
    print("=" * 50)

    # Inicializa o produtor Kafka (ou modo dry-run)
    producer = None
    if KAFKA_AVAILABLE:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8")
            )
            print(f"✅ Conectado ao Kafka em {KAFKA_BROKER}")
        except Exception as e:
            print(f"⚠️  Não foi possível conectar ao Kafka: {e}")
            print("   Rodando em modo DRY-RUN...")

    # Gera frota inicial
    fleet = generate_vehicle_fleet(n_vehicles=30)
    print(f"🚌 Frota inicializada com {len(fleet)} veículos")
    print(f"📍 Centro da cidade: ({LAT_CENTER}, {LON_CENTER})")
    print("\nPublicando eventos a cada 30 segundos (Ctrl+C para parar)...\n")

    event_count = 0

    try:
        while True:
            now = datetime.now(timezone.utc)
            peak = is_peak_hour(now.hour)

            batch_events = []

            for vehicle in fleet:
                # Move o veículo
                vehicle = move_vehicle(vehicle, peak)

                # Gera evento
                event = create_gps_event(vehicle, peak)
                event_dict = asdict(event)
                batch_events.append(event_dict)

                # Publica no Kafka ou imprime
                if producer:
                    producer.send(
                        TOPIC_GPS,
                        key=event.vehicle_id,
                        value=event_dict
                    )

            event_count += len(batch_events)

            # Resumo do batch
            avg_speed = sum(e["speed_kmh"] for e in batch_events) / len(batch_events)
            avg_occ = sum(e["occupancy_pct"] for e in batch_events) / len(batch_events)
            delayed = sum(1 for e in batch_events if e["status"] == "delayed")

            peak_label = "🔴 PICO" if peak else "🟢 Normal"
            print(f"[{now.strftime('%H:%M:%S')}] {peak_label} | "
                  f"Eventos: {len(batch_events)} | "
                  f"Vel. média: {avg_speed:.1f} km/h | "
                  f"Ocup. média: {avg_occ:.0f}% | "
                  f"Atrasados: {delayed} | "
                  f"Total: {event_count}")

            # Exemplo de evento
            if event_count <= len(fleet):
                print(f"   Exemplo: {json.dumps(batch_events[0], indent=None)}")

            if producer:
                producer.flush()

            time.sleep(30)  # Intervalo de 30 segundos (igual ao real)

    except KeyboardInterrupt:
        print(f"\n\n✅ Simulação encerrada. Total de eventos: {event_count}")
        if producer:
            producer.close()


if __name__ == "__main__":
    main()
