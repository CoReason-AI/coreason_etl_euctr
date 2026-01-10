# Copyright (c) 2025 CoReason, Inc.
#
# This software is proprietary and dual-licensed.
# Licensed under the Prosperity Public License 3.0 (the "License").
# A copy of the license is available at https://prosperitylicense.com/versions/3.0.0
# For details, see the LICENSE file.
# Commercial use beyond a 30-day trial requires a separate license.
#
# Source Code: https://github.com/CoReason-AI/coreason_etl_euctr

from pathlib import Path
from typing import List, Optional, Tuple

from coreason_etl_euctr.logger import logger
from coreason_etl_euctr.models import EuTrial, EuTrialCondition, EuTrialDrug
from coreason_etl_euctr.parser import Parser


def process_file_content(
    content: str, file_key: str, url_source: str
) -> Optional[Tuple[EuTrial, List[EuTrialDrug], List[EuTrialCondition]]]:
    """
    Process a single file content: Parse Trial, Drugs, and Conditions.
    Designed to be run in a separate process (must be picklable).
    """
    # Instantiate parser locally to avoid pickling external state if not needed,
    # or rely on it being stateless.
    parser = Parser()

    trial_id = Path(file_key).stem
    # Use a local logger bound to context, but loguru handles multiprocessing well.
    context_logger = logger.bind(trial_id=trial_id, file_key=file_key)

    try:
        try:
            trial = parser.parse_trial(content, url_source=url_source)
            # Ensure ID matches filename just in case
            if trial.eudract_number != trial_id:
                context_logger.warning(f"Filename {trial_id} mismatch with content {trial.eudract_number}")
        except ValueError as e:
            context_logger.warning(f"Failed to parse trial from {file_key}: {e}")
            return None

        trial_drugs = parser.parse_drugs(content, trial_id)
        trial_conds = parser.parse_conditions(content, trial_id)

        return (trial, trial_drugs, trial_conds)

    except Exception as e:
        context_logger.error(f"Error processing file {file_key}: {e}")
        return None
