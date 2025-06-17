import os
import sys
import threading
import time
import signal
import subprocess
from dotenv import load_dotenv

load_dotenv()

import requests
import pandas as pd
from datetime import datetime

from data_collector import NSEDataCollector
from indicator_calculator import IndicatorCalculator
from pipeline_validator import PipelineValidator
from data_preparation import DataPreparation
from schema_design import ensure_schema

from historical_downloader import process_nifty_input_file

def run_collector():
    collector = NSEDataCollector()
    collector.start()

def run_indicator_calculator():
    calc = IndicatorCalculator(mode="batch")
    calc.start()

def run_pipeline_validator():
    validator = PipelineValidator()
    validator.run_validation()

def run_data_preparation():
    prep = DataPreparation(llm_all_data=True)
    prep.run()

def main():
    ensure_schema()

    # --- Run historical batch downloader and TimescaleDB import ---
    process_nifty_input_file(
        input_file="Nifty_Input.csv",
        max_rows=2,
        api_delay=0.6,
        base_output_dir="historical_data"
    )

    # --- Ensure the scalping signal function exists before anything else ---
    dp = DataPreparation(llm_all_data=True)
    dp.create_scalping_signal_generator()

    # Start data collector and indicator calculator in threads
    t_collector = threading.Thread(target=run_collector, daemon=True)
    t_calc = threading.Thread(target=run_indicator_calculator, daemon=True)
    t_collector.start()
    t_calc.start()

    # Wait for indicator calculator to finish initial calculation (or sleep for a period)
    print("Waiting for indicator calculator to initialize...")
    time.sleep(10)

    # Run the pipeline validator synchronously (block until done)
    run_pipeline_validator()

    # Start data preparation in a separate thread (runs automatically, flexible LLM mode)
    t_prep = threading.Thread(target=run_data_preparation, daemon=True)
    t_prep.start()

    # --- LAUNCH STREAMLIT GUI ---
    gui_script = "app_gui.py"
    python_exe = sys.executable
    if os.path.exists(gui_script):
        try:
            subprocess.Popen(
                [python_exe, "-m", "streamlit", "run", gui_script],
                shell=True
            )
            print("Streamlit GUI is launching... (see your browser at http://localhost:8501)")
        except Exception as e:
            print(f"Could not launch Streamlit GUI automatically: {e}")
    else:
        print(f"GUI script {gui_script} not found. Please add it to your project directory.")

    def signal_handler(sig, frame):
        print("Shutting down all components...")
        sys.exit(0)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    while True:
        time.sleep(2)

if __name__ == "__main__":
    main()