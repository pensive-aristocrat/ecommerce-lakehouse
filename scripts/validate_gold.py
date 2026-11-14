"""Validate the Gold fact table with Great Expectations.

Run from the project root:  python scripts/validate_gold.py
Exit code 0 = all checks passed, 1 = at least one failed (Airflow uses this later).
"""
import sys

import great_expectations as gx
import pandas as pd
import trino

QUERY = """
    SELECT order_id, order_item_id, order_status,
           CAST(price AS double)         AS price,
           CAST(freight_value AS double) AS freight_value,
           review_score
    FROM fct_order_items
"""

ORDER_STATUSES = ["delivered", "shipped", "canceled", "unavailable",
                  "invoiced", "processing", "created", "approved"]


def load_from_trino(sql: str) -> pd.DataFrame:
    conn = trino.dbapi.connect(host="localhost", port=8080, user="gx",
                               catalog="lakehouse", schema="gold")
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description]
    conn.close()
    return pd.DataFrame(rows, columns=columns)


def main() -> int:
    df = load_from_trino(QUERY)
    print(f"Loaded {len(df):,} rows from gold.fct_order_items")

    context = gx.get_context(mode="ephemeral")
    batch = (context.data_sources.add_pandas("gold")
             .add_dataframe_asset("fct_order_items")
             .add_batch_definition_whole_dataframe("all_rows")
             .get_batch(batch_parameters={"dataframe": df}))

    suite = context.suites.add(gx.ExpectationSuite(name="fct_order_items_checks"))
    suite.add_expectation(gx.expectations.ExpectTableRowCountToBeBetween(
        min_value=100_000, max_value=130_000))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="order_id"))
    suite.add_expectation(gx.expectations.ExpectCompoundColumnsToBeUnique(
        column_list=["order_id", "order_item_id"]))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="price", min_value=0))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(
        column="review_score", min_value=1, max_value=5))
    suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(
        column="order_status", value_set=ORDER_STATUSES))
    suite.add_expectation(gx.expectations.ExpectColumnMeanToBeBetween(
        column="freight_value", min_value=10, max_value=40))

    result = batch.validate(suite)
    for check in result.results:
        status = "PASS" if check.success else "FAIL"
        column = check.expectation_config.kwargs.get("column", "")
        print(f"  {status}  {check.expectation_config.type:<45} {column}")
    print("All checks passed." if result.success else "Some checks FAILED.")
    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())