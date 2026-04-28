# rf_classification_template.py
from __future__ import annotations

import joblib
import numpy as np
import pandas as pd

from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.metrics import classification_report, confusion_matrix
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder


def build_pipeline(X: pd.DataFrame) -> Pipeline:
    # 列型で numeric / categorical を分ける（DataFrame前提）
    num_cols = X.select_dtypes(include=[np.number]).columns.tolist()
    cat_cols = X.select_dtypes(exclude=[np.number]).columns.tolist()

    numeric = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])

    categorical = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("onehot", OneHotEncoder(handle_unknown="ignore")),
    ])

    pre = ColumnTransformer(
        transformers=[
            ("num", numeric, num_cols),
            ("cat", categorical, cat_cols),
        ],
        remainder="drop",
    )

    rf = RandomForestClassifier(
        n_estimators=500,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced_subsample",
    )

    pipe = Pipeline(steps=[
        ("preprocess", pre),
        ("model", rf),
    ])
    return pipe


def main():
    # 例：CSVを読み込む想定（あなたのデータに置き換え）
    df = pd.read_csv("train.csv")
    target_col = "label"

    X = df.drop(columns=[target_col])
    y = df[target_col]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y,
        test_size=0.2,
        random_state=42,
        stratify=y
    )

    pipe = build_pipeline(X_train)

    # ハイパラ探索（最小限）
    param_grid = {
        "model__max_depth": [None, 10, 20],
        "model__min_samples_leaf": [1, 3, 10],
        "model__max_features": ["sqrt", 0.5, 1.0],
    }

    search = GridSearchCV(
        pipe,
        param_grid=param_grid,
        cv=5,
        scoring="f1_macro",
        n_jobs=-1,
        verbose=1,
    )
    search.fit(X_train, y_train)

    best = search.best_estimator_
    print("Best params:", search.best_params_)

    y_pred = best.predict(X_test)
    print("Confusion matrix:\n", confusion_matrix(y_test, y_pred))
    print(classification_report(y_test, y_pred, digits=4))

    # Permutation importance（“汎化”を見るなら test か検証データで計算）
    r = permutation_importance(
        best, X_test, y_test,
        n_repeats=10,
        random_state=42,
        n_jobs=-1,
    )
    # 重要度上位を表示（列名は元の列単位。OneHot後の詳細列を出すなら追加処理が必要）
    importances = pd.Series(r.importances_mean, index=X_test.columns).sort_values(ascending=False)
    print("\nPermutation importance (top 20):")
    print(importances.head(20))

    # モデル保存（Pipelineを丸ごと保存）
    joblib.dump(best, "rf_classifier_pipeline.joblib")
    print("\nSaved: rf_classifier_pipeline.joblib")


if __name__ == "__main__":
    main()
