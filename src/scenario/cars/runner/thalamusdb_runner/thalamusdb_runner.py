"""
ThalamusDB system runner implementation for Cars scenario.
"""

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from runner.generic_thalamusdb_runner.generic_thalamusdb_runner import GenericThalamusDBRunner


class ThalamusDBRunner(GenericThalamusDBRunner):
    def __init__(
        self,
        use_case: str,
        scale_factor: int,
        model_name: str = "gpt-4o-mini",
        skip_setup: bool = False,
    ):
        super().__init__(
            use_case, scale_factor, model_name
        )
