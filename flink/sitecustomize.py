"""Runtime patches for the local PyFlink environment.

This repo uses the upstream Flink image plus pip-installed Beam dependencies.
PyFlink 1.20's bundled worker code still defines FunctionOperation.setup(self),
while newer Beam worker startup paths call setup(self, data_sampler). Accept the
extra argument so Python UDF-based jobs can start reliably.
"""

from __future__ import annotations


def _patch_function_operation_setup() -> None:
    try:
        from pyflink.fn_execution.beam import beam_operations_slow
    except Exception:
        beam_operations_slow = None

    if beam_operations_slow is not None:
        original = beam_operations_slow.FunctionOperation.setup

        def patched(self, data_sampler=None):  # type: ignore[no-untyped-def]
            return original(self)

        beam_operations_slow.FunctionOperation.setup = patched

    try:
        from pyflink.fn_execution.beam import beam_operations_fast
    except Exception:
        beam_operations_fast = None

    if beam_operations_fast is not None and hasattr(beam_operations_fast, "FunctionOperation"):
        original = beam_operations_fast.FunctionOperation.setup

        def patched(self, data_sampler=None):  # type: ignore[no-untyped-def]
            return original(self)

        beam_operations_fast.FunctionOperation.setup = patched


_patch_function_operation_setup()
