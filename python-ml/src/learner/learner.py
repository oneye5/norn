from pathlib import Path
import pandas as pd
import joblib

from sklearn.ensemble import RandomForestClassifier, ExtraTreesClassifier, HistGradientBoostingClassifier, VotingClassifier
from xgboost import XGBClassifier
from lightgbm import LGBMClassifier

from src.utils.csv_utils import load_csv, save_csv
from src.utils.path_utils import get_skuld_root
from src.config.config import LABEL_COL


# =======================================================
# === MODEL FACTORY ====================================
# =======================================================

def build_default_model() -> VotingClassifier:
    """
    Assemble the default ensemble model (soft voting).
    Easily extensible by changing the estimators list or adding logic here.
    """
    estimators = [
        ('rf', RandomForestClassifier(random_state=42)),
        ('et', ExtraTreesClassifier(random_state=42)),
        ('hgb', HistGradientBoostingClassifier(random_state=42)),
        ('xgb', XGBClassifier(random_state=42)),
        ('lgbm', LGBMClassifier(random_state=42, verbose=-1)),
    ]
    model = VotingClassifier(estimators=estimators, voting='soft', n_jobs=-1)
    return model


# =======================================================
# === TRAINING PIPELINE =================================
# =======================================================

def train_model(train_csv_path: str, model_save_path: str):
    """
    Train a model using the specified training set and save it to disk.
    """
    df = load_csv(train_csv_path)

    # Split into X, y
    X = df.drop(columns=[LABEL_COL])
    y = df[LABEL_COL]

    # Build model
    model = build_default_model()

    # Fit model
    model.fit(X, y)

    # Save model
    joblib.dump(model, model_save_path)
    print(f"Model saved to {model_save_path}")


# =======================================================
# === PREDICTION PIPELINE ===============================
# =======================================================

def predict(model_path: str, input_csv_path: str, output_csv_path: str):
    """
    Load model and input data, generate ONLY probabilities, and save them.
    No discrete class labels are produced here.
    """
    # Load model
    model = joblib.load(model_path)

    # Load data
    df = load_csv(input_csv_path)

    # X only (drop label if present)
    X = df.drop(columns=[LABEL_COL]) if LABEL_COL in df.columns else df.copy()

    # Predict probabilities (only class 1 probability)
    df["pred_prob"] = model.predict_proba(X)[:, 1]

    save_csv(df, output_csv_path)
    print(f"Probability predictions saved to {output_csv_path}")



# =======================================================
# === ENTRYPOINT (Optional CLI Usage) ===================
# =======================================================

if __name__ == "__main__":
    root = get_skuld_root()

    # Default train/predict paths
    train_csv = root / "python-ml" / "data" / "train.csv"
    test_csv = root / "python-ml" / "data" / "test.csv"
    model_file = root / "python-ml" / "data" / "model.pkl"
    prediction_file = root / "python-ml" / "data" / "predictions.csv"

    # Train and predict automatically
    print("Training model...")
    train_model(str(train_csv), str(model_file))

    print("Generating predictions...")
    predict(str(model_file), str(test_csv), str(prediction_file))
