"""
Palimpzest system runner implementation for Cars scenario.
"""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from runner.generic_palimpzest_runner.generic_palimpzest_runner import GenericPalimpzestRunner


class PalimpzestRunner(GenericPalimpzestRunner):
    def __init__(
        self,
        use_case: str,
        scale_factor: int,
        model_name: str = "gpt-4o-mini",
        policy: str = "maxquality",
        skip_setup: bool = False,
    ):
        super().__init__(
            use_case, scale_factor, model_name, policy
        )
