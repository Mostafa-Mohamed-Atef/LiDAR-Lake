import json
import os
import sys
from typing import Dict, Tuple

from pyflink.common import Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


# === Inline noise_filters module ===
COORD_LIMITS = {
    "x": (-2000.0, 2000.0),
    "y": (-2000.0, 2000.0),
    "z": (-200.0, 500.0),
}

INTENSITY_RANGE = (0.0, 65535.0)
VALID_CLASSES = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9}


def _clamp(value: float, bounds: Tuple[float, float]) -> bool:
    low, high = bounds
    return low <= value <= high


def is_valid_point(point: Dict) -> Tuple[bool, str]:
    """
    Validate a LiDAR point dictionary.
    Returns (True, "") when the point is valid, otherwise (False, reason).
    """
    try:
        x = float(point["x"])
        y = float(point["y"])
        z = float(point["z"])
        intensity = float(point.get("intensity", 0))
        classification = int(point.get("classification", -1))
    except (ValueError, TypeError, KeyError) as exc:
        return False, f"parse_error:{exc}"

    if not _clamp(x, COORD_LIMITS["x"]) or not _clamp(y, COORD_LIMITS["y"]) or not _clamp(z, COORD_LIMITS["z"]):
        return False, "coord_out_of_bounds"

    if not _clamp(intensity, INTENSITY_RANGE):
        return False, "intensity_out_of_range"

    if classification not in VALID_CLASSES:
        return False, "invalid_class"

    return True, ""


# === Flink Job Configuration ===
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "lidar-stream")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "lidar-clean")
DIRTY_TOPIC = os.getenv("DIRTY_TOPIC", "lidar-dirty")
PARALLELISM = int(os.getenv("FLINK_PARALLELISM", "2"))


def build_env():
    """Build Flink StreamExecutionEnvironment with checkpointing"""
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    
    # Enable checkpointing for fault tolerance
    env.enable_checkpointing(
        interval=60_000,  # 60 seconds
        mode=CheckpointingMode.EXACTLY_ONCE,
    )
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30_000)
    
    return env


def kafka_consumer(topic: str, broker: str):
    """Build Kafka consumer using FlinkKafkaConsumer"""
    return FlinkKafkaConsumer(
        topics=topic,
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': broker, 'group.id': f'{topic}-cleaner'}
    )


def kafka_producer(topic: str, broker: str):
    """Build Kafka producer using FlinkKafkaProducer"""
    return FlinkKafkaProducer(
        topic=topic,
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': broker}
    )


class CleanPointMapper:
    """Stateless mapper to clean and validate LiDAR points"""
    
    def __call__(self, record: str) -> str:
        """
        Validate and enrich LiDAR point records.
        Adds _clean_result and _clean_reason fields.
        """
        try:
            data = json.loads(record)
            ok, reason = is_valid_point(data)
            data["_clean_result"] = "clean" if ok else "dirty"
            data["_clean_reason"] = reason if reason else "valid"
            return json.dumps(data)
        except Exception as e:
            # Handle malformed JSON
            return json.dumps({
                "_clean_result": "dirty",
                "_clean_reason": f"json_parse_error:{str(e)}"
            })


class CleanRecordFilter:
    """Filter for clean records"""
    
    def __call__(self, record: str) -> bool:
        try:
            data = json.loads(record)
            return data.get("_clean_result") == "clean"
        except:
            return False


class DirtyRecordFilter:
    """Filter for dirty records"""
    
    def __call__(self, record: str) -> bool:
        try:
            data = json.loads(record)
            return data.get("_clean_result") == "dirty"
        except:
            return True


def main():
    print(f"Starting LiDAR Cleaning Job", file=sys.stderr)
    print(f"Kafka Broker: {KAFKA_BROKER}", file=sys.stderr)
    print(f"Raw Topic: {RAW_TOPIC}", file=sys.stderr)
    print(f"Clean Topic: {CLEAN_TOPIC}", file=sys.stderr)
    print(f"Dirty Topic: {DIRTY_TOPIC}", file=sys.stderr)
    
    env = build_env()

    # Source: Read from Kafka raw LiDAR stream
    lidar_stream = env.add_source(kafka_consumer(RAW_TOPIC, KAFKA_BROKER))

    # Transform: Clean and validate points
    cleaned = lidar_stream.map(CleanPointMapper(), output_type=Types.STRING())

    # Split: Separate clean and dirty records
    clean_records = cleaned.filter(CleanRecordFilter())
    dirty_records = cleaned.filter(DirtyRecordFilter())

    # Sink: Send clean records to clean topic
    clean_records.add_sink(kafka_producer(CLEAN_TOPIC, KAFKA_BROKER))
    print(f"Clean records -> {CLEAN_TOPIC}", file=sys.stderr)

    # Sink: Send dirty records to dirty topic
    dirty_records.add_sink(kafka_producer(DIRTY_TOPIC, KAFKA_BROKER))
    print(f"Dirty records -> {DIRTY_TOPIC}", file=sys.stderr)

    # Execute the job
    env.execute("lidar-cleaning-job")


if __name__ == "__main__":
    main()

