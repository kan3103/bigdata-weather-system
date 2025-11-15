import argparse
import json
import math
import os
import random
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset

FEATURE_COLUMNS: List[str] = [
    "temperature",
    "windspeed",
    "winddirection",
    "humidity",
    "rain",
    "visibility",
    "pressure",
    "precipitation",
    "weathercode",
    "is_day",
    "interval",
]


def _read_json_records(path: Path) -> List[Dict]:
    if path.is_dir():
        part_files = sorted(path.glob("part-*.json"))
        if not part_files:
            raise FileNotFoundError(f"No part-*.json files found in directory {path}")
        records: List[Dict] = []
        for part in part_files:
            with part.open("r", encoding="utf-8") as fp:
                records.extend(json.loads(line) for line in fp if line.strip())
        return records

    with path.open("r", encoding="utf-8") as fp:
        return [json.loads(line) for line in fp if line.strip()]


class WeatherSequenceDataset(Dataset):
    def __init__(
        self,
        records: List[Dict],
        feature_columns: Optional[List[str]] = None,
        weathercode_mapping: Optional[Dict[int, int]] = None,
        *,
        balance_weather: bool = False,
        temperature_bin_size: Optional[float] = None,
        balance_max_replication: int = 3,
        seed: int = 42,
    ) -> None:
        self.samples: List[Tuple[torch.Tensor, int, float]] = []
        raw_sequences: List[Tuple[List[Dict], Dict]] = []
        codes: set[int] = set()
        candidate_feature_columns: Optional[set[str]] = None

        for record in records:
            sequence = record.get("sequence") or []
            target = record.get("target") or {}

            if not sequence:
                continue
            if target.get("temperature") is None or target.get("weathercode") is None:
                continue

            numeric_columns = {
                key
                for key, value in sequence[0].items()
                if key != "event_timestamp" and isinstance(value, (int, float))
            }
            if candidate_feature_columns is None:
                candidate_feature_columns = numeric_columns
            else:
                candidate_feature_columns &= numeric_columns

            try:
                weather_code = int(target["weathercode"])
            except (ValueError, TypeError):
                continue

            codes.add(weather_code)
            raw_sequences.append((sequence, target))

        if not raw_sequences:
            raise ValueError("No valid samples found in the provided records.")

        if feature_columns is not None:
            self.feature_columns = feature_columns
        else:
            candidate_feature_columns = candidate_feature_columns or set()
            inferred = [col for col in FEATURE_COLUMNS if col in candidate_feature_columns]
            if not inferred:
                inferred = sorted(candidate_feature_columns)
            if not inferred:
                inferred = ["temperature", "weathercode"]
            self.feature_columns = inferred

        if weathercode_mapping is None:
            unique_codes = sorted(codes)
            self.weathercode_to_idx = {code: idx for idx, code in enumerate(unique_codes)}
        else:
            self.weathercode_to_idx = weathercode_mapping

        remapped_samples: List[Tuple[torch.Tensor, int, float]] = []
        for sequence, target in raw_sequences:
            try:
                seq_array = np.array(
                    [
                        [
                            float(seq_step[col])
                            if (seq_step.get(col) is not None)
                            else math.nan
                            for col in self.feature_columns
                        ]
                        for seq_step in sequence
                    ],
                    dtype=np.float32,
                )
            except (ValueError, TypeError):
                continue

            if np.isnan(seq_array).any():
                continue

            weather_code = int(target["weathercode"])
            if weather_code not in self.weathercode_to_idx:
                continue

            remapped_samples.append(
                (
                    torch.from_numpy(seq_array),
                    self.weathercode_to_idx[weather_code],
                    float(target["temperature"]),
                )
            )

        if not remapped_samples:
            raise ValueError("No samples matched the provided weathercode mapping.")

        self.samples = remapped_samples

        self.samples = self._rebalance_samples(
            self.samples,
            balance_weather=balance_weather,
            temperature_bin_size=temperature_bin_size,
            max_replication=balance_max_replication,
            seed=seed,
        )

    def _rebalance_samples(
        self,
        samples: List[Tuple[torch.Tensor, int, float]],
        *,
        balance_weather: bool,
        temperature_bin_size: Optional[float],
        max_replication: int,
        seed: int,
    ) -> List[Tuple[torch.Tensor, int, float]]:
        if not samples:
            return samples
        if not balance_weather and (temperature_bin_size is None or temperature_bin_size <= 0):
            return samples

        rng = random.Random(seed)
        groups: Dict[Tuple[Optional[int], Optional[int]], List[Tuple[torch.Tensor, int, float]]] = defaultdict(list)

        def _temp_bin(value: float) -> Optional[int]:
            if temperature_bin_size is None or temperature_bin_size <= 0:
                return None
            return int(round(value / temperature_bin_size))

        for sequence_tensor, weather_idx, temperature in samples:
            key = (
                weather_idx if balance_weather else None,
                _temp_bin(temperature),
            )
            groups[key].append((sequence_tensor, weather_idx, temperature))

        if len(groups) <= 1:
            return samples

        largest_group = max(len(group_samples) for group_samples in groups.values())
        rebalanced: List[Tuple[torch.Tensor, int, float]] = []

        for group_samples in groups.values():
            target_size = largest_group
            if max_replication > 0:
                target_size = min(largest_group, len(group_samples) * max_replication)

            if target_size <= len(group_samples):
                rebalanced.extend(group_samples[:target_size])
            else:
                rebalanced.extend(group_samples)
                needed = target_size - len(group_samples)
                for _ in range(needed):
                    rebalanced.append(rng.choice(group_samples))

        rng.shuffle(rebalanced)
        return rebalanced

    def __len__(self) -> int:
        return len(self.samples)

    def __getitem__(self, index: int) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        sequence_tensor, weather_code_idx, temperature = self.samples[index]
        return (
            sequence_tensor,
            torch.tensor(weather_code_idx, dtype=torch.long),
            torch.tensor(temperature, dtype=torch.float32),
        )


class WeatherForecastModel(nn.Module):
    def __init__(
        self,
        input_dim: int,
        hidden_dim: int,
        num_layers: int,
        num_weather_codes: int,
        dropout: float,
    ) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_dim,
            hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.dropout = nn.Dropout(dropout)
        self.weather_head = nn.Linear(hidden_dim, num_weather_codes)
        self.temperature_head = nn.Linear(hidden_dim, 1)

    def forward(self, inputs: torch.Tensor) -> Tuple[torch.Tensor, torch.Tensor]:
        lstm_out, _ = self.lstm(inputs)
        last_state = lstm_out[:, -1, :]
        last_state = self.dropout(last_state)
        weather_logits = self.weather_head(last_state)
        temperature_pred = self.temperature_head(last_state).squeeze(-1)
        return weather_logits, temperature_pred


def _collate_batch(
    batch: List[Tuple[torch.Tensor, torch.Tensor, torch.Tensor]]
) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
    sequences, weather_idx, temperature = zip(*batch)
    return (
        torch.stack(sequences, dim=0),
        torch.stack(weather_idx, dim=0),
        torch.stack(temperature, dim=0),
    )


class DatasetSplit(Dataset):
    def __init__(self, base: WeatherSequenceDataset, indices: List[int]) -> None:
        self.base = base
        self.indices = indices
        self.feature_columns = base.feature_columns
        self.weathercode_to_idx = base.weathercode_to_idx

    def __len__(self) -> int:
        return len(self.indices)

    def __getitem__(self, idx: int) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return self.base[self.indices[idx]]


def _evaluate_losses(
    model: WeatherForecastModel,
    data_loader: DataLoader,
    weather_loss_fn: nn.Module,
    temperature_loss_fn: nn.Module,
    temperature_loss_weight: float,
    device: str,
) -> Tuple[float, float, float]:
    model.eval()
    total_loss = 0.0
    total_weather_loss = 0.0
    total_temp_loss = 0.0
    sample_count = 0

    with torch.no_grad():
        for batch_sequences, batch_weather_idx, batch_temperature in data_loader:
            batch_sequences = batch_sequences.to(device)
            batch_weather_idx = batch_weather_idx.to(device)
            batch_temperature = batch_temperature.to(device)

            weather_logits, temperature_pred = model(batch_sequences)
            weather_loss = weather_loss_fn(weather_logits, batch_weather_idx)
            temperature_loss = temperature_loss_fn(temperature_pred, batch_temperature)
            loss = weather_loss + temperature_loss_weight * temperature_loss

            batch_size = batch_sequences.size(0)
            total_loss += loss.item() * batch_size
            total_weather_loss += weather_loss.item() * batch_size
            total_temp_loss += temperature_loss.item() * batch_size
            sample_count += batch_size

    if sample_count == 0:
        return 0.0, 0.0, 0.0
    return (
        total_loss / sample_count,
        total_weather_loss / sample_count,
        total_temp_loss / sample_count,
    )


def train_model(
    train_dataset: Dataset,
    val_dataset: Optional[Dataset],
    *,
    batch_size: int = 32,
    epochs: int = 10,
    learning_rate: float = 1e-3,
    hidden_dim: int = 128,
    num_layers: int = 2,
    dropout: float = 0.2,
    temperature_loss_weight: float = 0.1,
    weight_decay: float = 0.0,
    patience: int = 5,
    device: Optional[str] = None,
) -> Tuple[WeatherForecastModel, Dict[str, List[float]]]:
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(
        train_dataset,
        batch_size=batch_size,
        shuffle=True,
        drop_last=False,
        collate_fn=_collate_batch,
    )
    val_loader = (
        DataLoader(
            val_dataset,
            batch_size=batch_size,
            shuffle=False,
            drop_last=False,
            collate_fn=_collate_batch,
        )
        if val_dataset is not None
        else None
    )

    base_dataset: WeatherSequenceDataset = (
        train_dataset.base if isinstance(train_dataset, DatasetSplit) else train_dataset
    )

    model = WeatherForecastModel(
        input_dim=len(base_dataset.feature_columns),
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        num_weather_codes=len(base_dataset.weathercode_to_idx),
        dropout=dropout,
    ).to(device)

    optimizer = optim.Adam(
        model.parameters(), lr=learning_rate, weight_decay=weight_decay
    )
    weather_loss_fn = nn.CrossEntropyLoss()
    temperature_loss_fn = nn.MSELoss()

    history = {
        "loss": [],
        "weather_loss": [],
        "temperature_loss": [],
        "val_loss": [],
        "val_weather_loss": [],
        "val_temperature_loss": [],
    }

    best_val_loss = float("inf")
    patience_counter = 0
    best_state_dict = None

    for epoch in range(1, epochs + 1):
        model.train()
        total_loss = 0.0
        total_weather_loss = 0.0
        total_temp_loss = 0.0
        sample_count = 0

        for batch_sequences, batch_weather_idx, batch_temperature in train_loader:
            batch_sequences = batch_sequences.to(device)
            batch_weather_idx = batch_weather_idx.to(device)
            batch_temperature = batch_temperature.to(device)

            optimizer.zero_grad()
            weather_logits, temperature_pred = model(batch_sequences)
            weather_loss = weather_loss_fn(weather_logits, batch_weather_idx)
            temperature_loss = temperature_loss_fn(temperature_pred, batch_temperature)
            loss = weather_loss + temperature_loss_weight * temperature_loss

            loss.backward()
            optimizer.step()

            batch_size_actual = batch_sequences.size(0)
            total_loss += loss.item() * batch_size_actual
            total_weather_loss += weather_loss.item() * batch_size_actual
            total_temp_loss += temperature_loss.item() * batch_size_actual
            sample_count += batch_size_actual

        train_loss = total_loss / sample_count
        train_weather_loss = total_weather_loss / sample_count
        train_temp_loss = total_temp_loss / sample_count

        history["loss"].append(train_loss)
        history["weather_loss"].append(train_weather_loss)
        history["temperature_loss"].append(train_temp_loss)

        if val_loader is not None:
            val_loss, val_weather_loss, val_temp_loss = _evaluate_losses(
                model,
                val_loader,
                weather_loss_fn,
                temperature_loss_fn,
                temperature_loss_weight,
                device,
            )
            history["val_loss"].append(val_loss)
            history["val_weather_loss"].append(val_weather_loss)
            history["val_temperature_loss"].append(val_temp_loss)
            print(
                f"Epoch {epoch:02d}/{epochs} "
                f"- loss: {train_loss:.4f} (weather: {train_weather_loss:.4f}, temp: {train_temp_loss:.4f}) "
                f"- val_loss: {val_loss:.4f} (weather: {val_weather_loss:.4f}, temp: {val_temp_loss:.4f})"
            )

            if val_loss < best_val_loss - 1e-4:
                best_val_loss = val_loss
                best_state_dict = model.state_dict()
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    print(
                        f"Early stopping triggered (no improvement in val_loss for {patience} epochs)."
                    )
                    break
        else:
            print(
                f"Epoch {epoch:02d}/{epochs} "
                f"- loss: {train_loss:.4f} (weather: {train_weather_loss:.4f}, temp: {train_temp_loss:.4f})"
            )

    if best_state_dict is not None:
        model.load_state_dict(best_state_dict)

    return model, history


def evaluate_model(
    model: WeatherForecastModel,
    dataset: WeatherSequenceDataset,
    *,
    batch_size: int = 64,
    device: Optional[str] = None,
) -> Dict[str, object]:
    device = device or ("cuda" if torch.cuda.is_available() else "cpu")
    data_loader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=False,
        drop_last=False,
        collate_fn=_collate_batch,
    )

    model.eval()
    weather_correct = 0
    weather_total = 0
    temperature_mae = 0.0
    true_temps: List[float] = []
    pred_temps: List[float] = []
    true_codes: List[int] = []
    pred_codes: List[int] = []
    idx_to_weather = {idx: code for code, idx in dataset.weathercode_to_idx.items()}

    with torch.no_grad():
        for batch_sequences, batch_weather_idx, batch_temperature in data_loader:
            batch_sequences = batch_sequences.to(device)
            batch_weather_idx = batch_weather_idx.to(device)
            batch_temperature = batch_temperature.to(device)

            weather_logits, temperature_pred = model(batch_sequences)
            predicted_codes = weather_logits.argmax(dim=-1)

            weather_correct += (predicted_codes == batch_weather_idx).sum().item()
            weather_total += batch_sequences.size(0)

            temperature_mae += torch.abs(temperature_pred - batch_temperature).sum().item()

            pred_temps.extend(temperature_pred.cpu().tolist())
            true_temps.extend(batch_temperature.cpu().tolist())
            pred_codes.extend(
                idx_to_weather[int(idx)] for idx in predicted_codes.cpu().tolist()
            )
            true_codes.extend(
                idx_to_weather[int(idx)] for idx in batch_weather_idx.cpu().tolist()
            )

    weather_accuracy = weather_correct / max(weather_total, 1)
    temperature_mae /= max(weather_total, 1)

    print(f"Weathercode accuracy: {weather_accuracy:.4f}")
    print(f"Temperature MAE: {temperature_mae:.4f}")

    return {
        "weather_accuracy": weather_accuracy,
        "temperature_mae": temperature_mae,
        "true_temperatures": true_temps,
        "pred_temperatures": pred_temps,
        "true_weather_codes": true_codes,
        "pred_weather_codes": pred_codes,
    }


def plot_training_diagnostics(
    history: Dict[str, List[float]],
    evaluation: Dict[str, object],
    output_path: Path,
) -> None:
    try:
        import matplotlib.pyplot as plt  # type: ignore[import]
    except ImportError as exc:
        raise RuntimeError(
            "Matplotlib is required for visualization. Install it via 'pip install matplotlib'."
        ) from exc

    epochs = range(1, len(history["loss"]) + 1)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    axes[0].plot(epochs, history["loss"], label="Train total loss")
    axes[0].plot(epochs, history["weather_loss"], label="Train weathercode loss")
    axes[0].plot(epochs, history["temperature_loss"], label="Train temperature loss")
    if history["val_loss"]:
        axes[0].plot(epochs[: len(history["val_loss"])], history["val_loss"], label="Val total loss")
        axes[0].plot(
            epochs[: len(history["val_weather_loss"])],
            history["val_weather_loss"],
            label="Val weathercode loss",
        )
        axes[0].plot(
            epochs[: len(history["val_temperature_loss"])],
            history["val_temperature_loss"],
            label="Val temperature loss",
        )
    axes[0].set_xlabel("Epoch")
    axes[0].set_ylabel("Loss")
    axes[0].set_title("Training Loss Curves")
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)

    true_temp = np.array(evaluation["true_temperatures"])
    pred_temp = np.array(evaluation["pred_temperatures"])
    sample_count = min(200, len(true_temp))
    axes[1].scatter(
        true_temp[:sample_count],
        pred_temp[:sample_count],
        s=12,
        alpha=0.6,
        edgecolors="none",
    )
    if sample_count > 0:
        min_temp = min(true_temp[:sample_count].min(), pred_temp[:sample_count].min())
        max_temp = max(true_temp[:sample_count].max(), pred_temp[:sample_count].max())
        axes[1].plot([min_temp, max_temp], [min_temp, max_temp], "r--", linewidth=1)
    axes[1].set_xlabel("True temperature")
    axes[1].set_ylabel("Predicted temperature")
    axes[1].set_title(
        f"Temperature Prediction (MAE={evaluation['temperature_mae']:.2f})"
    )
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(output_path)
    plt.close(fig)
    print(f"Saved training diagnostics to {output_path.resolve()}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train an LSTM to predict future weathercode and temperature."
    )
    parser.add_argument(
        "--data-path",
        default=os.getenv(
            "WEATHER_SEQUENCE_DATA",
            "prediction/datasets/sample_weather.json",
        ),
        help="Path to the JSON dataset produced by getSample.py (file or directory).",
    )
    parser.add_argument("--epochs", type=int, default=15, help="Number of training epochs.")
    parser.add_argument("--batch-size", type=int, default=64, help="Training batch size.")
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=1e-3,
        help="Learning rate for the optimizer.",
    )
    parser.add_argument(
        "--hidden-dim",
        type=int,
        default=128,
        help="Hidden dimension size for the LSTM.",
    )
    parser.add_argument(
        "--num-layers",
        type=int,
        default=2,
        help="Number of LSTM layers.",
    )
    parser.add_argument(
        "--dropout",
        type=float,
        default=0.2,
        help="Dropout rate applied after the LSTM (default: 0.2).",
    )
    parser.add_argument(
        "--temperature-loss-weight",
        type=float,
        default=0.1,
        help="Relative weight applied to temperature regression loss.",
    )
    parser.add_argument(
        "--weight-decay",
        type=float,
        default=1e-5,
        help="Weight decay (L2 regularization) for the optimizer (default: 1e-5).",
    )
    parser.add_argument(
        "--val-split",
        type=float,
        default=0.2,
        help="Fraction of samples reserved for validation (default: 0.2).",
    )
    parser.add_argument(
        "--patience",
        type=int,
        default=5,
        help="Number of epochs with no val improvement before early stopping (default: 5).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--balance-weather",
        action="store_true",
        help="Balance the dataset by weathercode frequency via oversampling.",
    )
    parser.add_argument(
        "--balance-temperature-bin",
        type=float,
        default=0.0,
        help="Bin width (in °C) used to balance temperature distribution via oversampling. Set to 0 to disable.",
    )
    parser.add_argument(
        "--balance-max-factor",
        type=int,
        default=3,
        help="Maximum replication factor per group when balancing (default: 3).",
    )
    parser.add_argument(
        "--save-model",
        default=os.getenv("OUTPUT_MODEL_PATH"),
        help="Optional path to save the trained model state dict.",
    )
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Generate training diagnostics plot (requires matplotlib).",
    )
    parser.add_argument(
        "--visualize-path",
        default=os.getenv("WEATHER_VIS_PATH", "prediction/models/training_diagnostics.png"),
        help="Destination file for diagnostics plot when --visualize is enabled.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not 0.0 <= args.val_split < 1.0:
        raise ValueError("val_split must be in the range [0.0, 1.0).")
    if args.dropout < 0.0 or args.dropout >= 1.0:
        raise ValueError("dropout must be in the range [0.0, 1.0).")

    random.seed(args.seed)
    np.random.seed(args.seed)
    torch.manual_seed(args.seed)

    data_path = Path(args.data_path)
    records = _read_json_records(data_path)

    dataset = WeatherSequenceDataset(
        records,
        balance_weather=args.balance_weather,
        temperature_bin_size=(
            args.balance_temperature_bin
            if args.balance_temperature_bin and args.balance_temperature_bin > 0
            else None
        ),
        balance_max_replication=max(1, args.balance_max_factor),
        seed=args.seed,
    )
    print(
        f"Loaded {len(dataset)} samples across {len(dataset.weathercode_to_idx)} weather codes "
        f"(after balancing)."
    )

    indices = list(range(len(dataset)))
    np.random.shuffle(indices)
    val_size = int(len(indices) * args.val_split)
    if val_size > 0:
        val_indices = indices[:val_size]
        train_indices = indices[val_size:]
        val_dataset: Optional[Dataset] = DatasetSplit(dataset, val_indices)
    else:
        train_indices = indices
        val_dataset = None
    train_dataset: Dataset = DatasetSplit(dataset, train_indices)

    print(
        f"Training samples: {len(train_dataset)} "
        f"| Validation samples: {len(val_dataset) if val_dataset else 0}"
    )

    model, history = train_model(
        train_dataset,
        val_dataset,
        batch_size=args.batch_size,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        hidden_dim=args.hidden_dim,
        num_layers=args.num_layers,
        dropout=args.dropout,
        temperature_loss_weight=args.temperature_loss_weight,
        weight_decay=args.weight_decay,
        patience=args.patience,
    )

    eval_dataset = val_dataset if val_dataset is not None else train_dataset
    evaluation = evaluate_model(model, eval_dataset)

    if args.visualize:
        plot_path = Path(args.visualize_path)
        plot_path.parent.mkdir(parents=True, exist_ok=True)
        plot_training_diagnostics(history, evaluation, plot_path)

    if args.save_model:
        save_path = Path(args.save_model)
        save_path.parent.mkdir(parents=True, exist_ok=True)
        torch.save(
            {
                "model_state_dict": model.state_dict(),
                "feature_columns": dataset.feature_columns,
                "weathercode_to_idx": dataset.weathercode_to_idx,
            },
            save_path,
        )
        print(f"Saved model state to {save_path.resolve()}")


if __name__ == "__main__":
    main()
