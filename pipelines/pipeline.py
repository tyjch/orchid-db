import os
import sys
import subprocess
from pathlib import Path
import psycopg
from dotenv import load_dotenv
from metaflow import FlowSpec, step, Parameter, IncludeFile, current
from loguru import logger

load_dotenv()

class TaxonomyPipeline(FlowSpec):
  pass