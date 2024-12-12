from pathlib import Path

import pandas as pd


def save_to_parquet(data: list[dict], output_path: str) -> None:
    if not data:
        raise ValueError("The data list is empty. Cannot save an empty dataset.")

    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(data)

    df.to_parquet(output_file, index=False)
