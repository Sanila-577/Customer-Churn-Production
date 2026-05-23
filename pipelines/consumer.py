#!/usr/bin/env python3
"""
Simplified Kafka Consumer with ML Predictions
Processes customer events with real-time ML inference
"""

import json
import logging
import argparse
import os
import sys
import time
import subprocess
from typing import Dict, Any
from datetime import datetime

# Add project root to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from confluent_kafka import Consumer, Producer, KafkaError
from src.model_inference import ModelInference

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from utils.config import load_config

# Constants (config-driven)
cfg = load_config().get('kafka', {})
INPUT_TOPIC = cfg.get('consumer', {}).get('input_topic', cfg.get('topics', {}).get('raw_input', 'telco.raw.customers'))
OUTPUT_TOPIC = cfg.get('consumer', {}).get('output_topic', cfg.get('topics', {}).get('predictions_output', 'telco.churn.predictions'))
MODEL_PATH = "artifacts/models/spark_random_forest_model"


class MLKafkaConsumer:
    """Simplified ML Kafka Consumer"""
    
    def __init__(self):
        self.model = None

    def _configure_airflow_environment(self):
        """Point Airflow commands at the project-local metadata database."""
        airflow_home = os.path.join(project_root, ".airflow")
        venv_bin = os.path.join(project_root, ".venv", "bin")
        os.makedirs(os.path.join(airflow_home, "dags"), exist_ok=True)
        os.makedirs(os.path.join(airflow_home, "logs"), exist_ok=True)
        os.environ.setdefault("AIRFLOW_HOME", airflow_home)
        os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
        existing_pythonpath = os.environ.get("PYTHONPATH")
        os.environ["PYTHONPATH"] = (
            f"{project_root}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else project_root
        )
        os.environ["PATH"] = f"{venv_bin}{os.pathsep}{os.environ.get('PATH', '')}"
        
    def initialize(self):
        """Initialize ML model"""
        try:
            self.model = ModelInference(model_path=MODEL_PATH, use_spark=False)
            
            # Load encoders
            encoders_dir = "artifacts/encode"
            if os.path.exists(encoders_dir):
                self.model.load_encoders(encoders_dir)
                logger.info("✅ ML model and encoders loaded")
            
            return True
        except Exception as e:
            logger.error(f"❌ Initialization failed: {str(e)}")
            return False
    
    def extract_customer_data(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract and validate customer data"""
        # Handle nested structure
        customer_data = message_data.get('data', message_data)
        
        # Required fields with defaults
        return {
            'CustomerId': customer_data.get('CustomerId', 0),
            'Geography': customer_data.get('Geography', 'Unknown'),
            'Gender': customer_data.get('Gender', 'Unknown'),
            'Age': customer_data.get('Age', 0),
            'CreditScore': customer_data.get('CreditScore', 600),
            'Balance': customer_data.get('Balance', 0.0),
            'EstimatedSalary': customer_data.get('EstimatedSalary', 0.0),
            'Tenure': customer_data.get('Tenure', 0),
            'NumOfProducts': customer_data.get('NumOfProducts', 1),
            'HasCrCard': customer_data.get('HasCrCard', 0),
            'IsActiveMember': customer_data.get('IsActiveMember', 0),
            'Exited': customer_data.get('Exited', 0)
        }
    
    def process_batch(self, max_messages: int = 1000, timeout: int = 10, 
                     group_id: str = None) -> int:
        """Trigger the Airflow DAG that performs Kafka consumption and scoring.

        This path intentionally does not fall back to local consumption. The
        DAG is the source of truth for consuming and scoring messages.
        """
        self._configure_airflow_environment()

        # Try Airflow local client first
        try:
            from airflow.api.client.local_client import Client
            client = Client(None, None)
            client.trigger_dag(dag_id='kafka_consumer_dag', run_id=None, conf={
                'max_messages': max_messages,
                'timeout': timeout
            })
            logger.info("🚀 Triggered Airflow DAG: kafka_consumer_dag")
            return 0
        except Exception as e:
            logger.warning(f"⚠️ Airflow local client trigger failed: {e}")

        # Try Airflow CLI
        try:
            conf = json.dumps({'max_messages': max_messages, 'timeout': timeout})
            subprocess.run(["airflow", "dags", "trigger", "kafka_consumer_dag", "--conf", conf], check=True)
            logger.info("🚀 Triggered Airflow DAG via CLI: kafka_consumer_dag")
            return 0
        except Exception as e:
            raise RuntimeError(f"Unable to trigger Airflow DAG kafka_consumer_dag: {e}")

    def _consume_messages(self, max_messages: int = 1000, timeout: int = 10, group_id: str = None):
        """Consume raw messages from Kafka and return list of parsed JSON payloads."""
        # Configure consumer
        if group_id is None:
            group_id = f"batch_consumer_{int(time.time())}"

        consumer_config = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group_id,
            'auto.offset.reset': 'earliest' if 'batch_' in group_id else 'latest',
            'enable.auto.commit': True
        }

        consumer = Consumer(consumer_config)
        consumer.subscribe([INPUT_TOPIC])

        messages = []
        start_time = time.time()
        while len(messages) < max_messages and (time.time() - start_time) < timeout:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                continue
            try:
                message_data = json.loads(msg.value().decode('utf-8'))
                messages.append(message_data)
            except json.JSONDecodeError:
                continue

        consumer.close()
        return messages

    def _process_messages(self, messages: list) -> int:
        """Process a list of message payloads with the model and produce scored results."""
        if not messages:
            return 0

        logger.info(f"📥 Processing {len(messages)} messages with ML")
        producer = Producer({'bootstrap.servers': 'localhost:9092'})
        processed = 0

        print(f"\n📊 ML PREDICTIONS")
        print("=" * 70)
        print("Status | Customer   | Location | Prediction | Confidence")
        print("-" * 70)

        for i, message_data in enumerate(messages):
            try:
                customer_data = self.extract_customer_data(message_data)
                customer_id = customer_data.get('CustomerId', 'N/A')
                geography = str(customer_data.get('Geography', 'N/A'))[:5]

                prediction = self.model.predict(customer_data)
                status = prediction.get('Status', 'Unknown')
                confidence = prediction.get('Confidence', '0%')

                pred_emoji = "🟢" if 'Retain' in status else "🔴"
                print(f"  {pred_emoji}   | {str(customer_id)[:8]:8s} | {geography:8s} | {status:10s} | {confidence:10s}")

                result = {
                    'customer_id': customer_id,
                    'original_data': customer_data,
                    'prediction': prediction,
                    'processed_at': datetime.now().isoformat(),
                    'batch_id': f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
                }

                producer.produce(
                    topic=OUTPUT_TOPIC,
                    key=str(customer_id),
                    value=json.dumps(result, default=str)
                )

                processed += 1

            except Exception as e:
                print(f"  ❌   | ERROR    | ERROR    | FAILED     | ERROR")
                logger.error(f"Error processing message {i}: {str(e)}")

        producer.flush()

        print("-" * 70)
        print(f"✅ Completed: {processed}/{len(messages)} predictions")
        print("=" * 70)

        logger.info(f"🎉 Processed {processed} messages successfully")
        return processed
    
    def run_continuous(self, poll_interval: int = 3, show_progress: bool = True):
        """Keep the process alive while Airflow handles scheduled consumption."""
        logger.info("🔄 Starting Airflow-backed continuous consumer mode")
        logger.info("🛑 Press Ctrl+C to stop")

        self.process_batch(
            max_messages=50,
            timeout=poll_interval,
            group_id='continuous_ml_consumer'
        )

        if show_progress:
            print("✅ Airflow DAG kafka_consumer_dag triggered. The scheduler will consume batches.")
        
        try:
            while True:
                if show_progress:
                    print("⏳ Waiting for Airflow scheduler to run kafka_consumer_dag...")
                time.sleep(poll_interval)
                
        except KeyboardInterrupt:
            logger.info("🛑 Continuous processing stopped")

    @classmethod
    def run_kafka_consumer_batch(cls, max_messages: int = 1000, timeout: int = 10, group_id: str = None) -> int:
        """Class-level entrypoint for Airflow / external callers.

        Creates an instance, initializes it, consumes messages and processes them.
        Keeps behavior identical to the previous standalone function but lives
        on the class for better encapsulation and testability.
        """
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        # Ensure working directory is project root for relative paths
        try:
            os.chdir(project_root)
        except Exception:
            pass

        inst = cls()
        if not inst.initialize():
            raise RuntimeError("Failed to initialize Kafka ML consumer")

        messages = inst._consume_messages(max_messages=max_messages, timeout=timeout, group_id=group_id)
        processed = inst._process_messages(messages)

        return processed


def main():
    """Main function"""
    parser = argparse.ArgumentParser(description="Kafka Consumer with ML Predictions")
    parser.add_argument('--max-messages', type=int, default=1000)
    parser.add_argument('--timeout', type=int, default=10)
    parser.add_argument('--continuous', action='store_true')
    parser.add_argument('--poll-interval', type=int, default=3)
    parser.add_argument('--quiet', action='store_true')
    
    args = parser.parse_args()
    
    try:
        logger.info("🚀 Starting Kafka ML Consumer")
        
        consumer = MLKafkaConsumer()
        if not consumer.initialize():
            return 1
        
        if args.continuous:
            consumer.run_continuous(args.poll_interval, not args.quiet)
        else:
            consumer.process_batch(args.max_messages, args.timeout)
            return 0
        
    except Exception as e:
        logger.error(f"❌ Consumer failed: {str(e)}")
        return 1


if __name__ == "__main__":
    exit(main())