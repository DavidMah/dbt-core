import pytest
import os

from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture

str_case_sql = """
    {{ log('str987654321') }}
    select NULL
"""

int_case_sql = """
    {{ log(987654321) }}
    select NULL
"""

float_case_sql = """
    {{ log(9876.54321) }}
    select NULL
"""

list_case_sql = """
    {{ log([9, 8, 7, 6, 5, 4, 3, 2, 1]) }}
    select NULL
"""

dict_case_sql = """
    {{ log({9: 8}) }}
    select NULL
"""


class TestLogArbitraryTypes:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "str_case.sql": str_case_sql,
            "int_case.sql": int_case_sql,
            "float_case.sql": float_case_sql,
            "list_case.sql": list_case_sql,
            "dict_case.sql": dict_case_sql,
        }

    def test_log(self, project):
        # Induce dbt to try scrubbing logs, which fails if not given strings
        os.environ["DBT_ENV_SECRET_WHATEVER"] = "1234"
        os.environ["DBT_DEBUG"] = "True"
        _, log_output = run_dbt_and_capture(["run"])

        # These aren't theoretically sound ways to verify the output
        # but they should be sufficient for this minor test.
        assert " str987654321\n" in log_output
        assert " 987654321\n" in log_output
        assert " 9876.54321\n" in log_output
        assert " [9, 8, 7, 6, 5, 4, 3, 2, 1]\n" in log_output
        assert " {9: 8}\n" in log_output
