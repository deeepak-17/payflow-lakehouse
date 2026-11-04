"""Fail (exit 1) if any DAG in the given folder has an import error.

`airflow dags list-import-errors` exits 0 even when errors exist and needs a
migrated metadata DB, so CI parses the folder directly with DagBag instead.
"""
import sys
from airflow.models.dagbag import DagBag
bag = DagBag(dag_folder=sys.argv[1], include_examples=False)
if bag.import_errors:
    for path, err in bag.import_errors.items():
        print(f"{path}:\n{err}", file=sys.stderr)
    sys.exit(1)
if not bag.dags:
    sys.exit("no DAGs found")
print("DAGs OK:", sorted(bag.dag_ids))
