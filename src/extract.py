import os
import pandas as pd


RAW_DATA_PATH = "data/raw/DataEngineer.csv"
PREVIEW_OUTPUT_PATH = "data/processed/raw_preview.csv"


def extract_job_data(file_path: str) -> pd.DataFrame:
    """
    Extract raw job posting data from a CSV file.

    Args:
        file_path: Path to the raw CSV file.

    Returns:
        A pandas DataFrame containing the raw job posting data.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(
            f"File not found: {file_path}\n"
            "Please make sure DataEngineer.csv is inside data/raw/"
        )

    df = pd.read_csv(file_path)

    return df


def inspect_data(df: pd.DataFrame) -> None:
    """
    Print basic information about the dataset.
    This helps us understand the raw data before cleaning.
    """
    print("\n========== DATASET OVERVIEW ==========")
    print(f"Number of rows: {df.shape[0]}")
    print(f"Number of columns: {df.shape[1]}")

    print("\n========== COLUMN NAMES ==========")
    for col in df.columns:
        print(f"- {col}")

    print("\n========== FIRST 5 ROWS ==========")
    print(df.head())

    print("\n========== MISSING VALUES ==========")
    print(df.isnull().sum())

    print("\n========== DUPLICATE ROWS ==========")
    print(f"Number of duplicate rows: {df.duplicated().sum()}")

    print("\n========== DATA TYPES ==========")
    print(df.dtypes)


def save_preview(df: pd.DataFrame, output_path: str, n: int = 20) -> None:
    """
    Save a small preview of the raw data for quick inspection.

    Args:
        df: Raw DataFrame.
        output_path: Path to save the preview CSV.
        n: Number of rows to save.
    """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    df.head(n).to_csv(output_path, index=False)

    print(f"\nPreview saved to: {output_path}")


def main():
    """
    Main extraction workflow:
    1. Read raw CSV
    2. Inspect basic dataset information
    3. Save a small preview file
    """
    print("Starting extraction...")

    df = extract_job_data(RAW_DATA_PATH)

    print("Raw data loaded successfully.")

    inspect_data(df)

    save_preview(df, PREVIEW_OUTPUT_PATH)

    print("\nExtraction completed successfully.")


if __name__ == "__main__":
    main()