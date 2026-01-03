"""
Lotus system runner implementation for Cars scenario.
"""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from runner.generic_lotus_runner.generic_lotus_runner import GenericLotusRunner


class LotusRunner(GenericLotusRunner):
    def __init__(
        self,
        use_case: str,
        scale_factor: int,
        model_name: str = "gemini-2.5-pro",
        concurrent_llm_worker=20,
        skip_setup: bool = False,
    ):
        super().__init__(
            use_case, scale_factor, model_name, concurrent_llm_worker
        )
