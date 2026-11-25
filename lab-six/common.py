import os

import numpy as np
import torch
from torch import nn
from torch.utils.data import TensorDataset

ROOT = os.path.dirname(os.path.abspath(__file__))

DATASETS = {
    "Д1": ("dermamnist", 7, 3),
    "Д2": ("bloodmnist", 8, 3),
    "Д3": ("organamnist", 11, 1),
    "Д4": ("organcmnist", 11, 1),
    "Д5": ("pathmnist", 9, 3),
    "Д6": ("octmnist", 4, 1),
}
WIDTHS = {"Ш1": (16, 32, 128), "Ш2": (32, 64, 256), "Ш3": (64, 128, 512)}

DATASET, WIDTH = "Д6", "Ш2"
P_MAX, B, K_C, S_F = 4, 96, 10, 25
N_TRAIN = 7000
LR0, MOMENTUM, EPOCHS = 0.02, 0.9, 3
WARMUP_STEPS, MEASURE_STEPS = 10, 50
C_PHYS = os.cpu_count() if os.uname().sysname != "Darwin" else int(
    os.popen("sysctl -n hw.physicalcpu").read().strip())
N_TH = max(C_PHYS // P_MAX, 1)


def load_split(path, split, n=None):
    d = np.load(path)
    x = d[f"{split}_images"][:n]
    y = d[f"{split}_labels"][:n].reshape(-1)
    if x.ndim == 3:
        x = x[:, None, :, :]
    else:
        x = x.transpose(0, 3, 1, 2)
    x = torch.from_numpy(np.ascontiguousarray(x)).float()
    x = (x / 255 - 0.5) / 0.5
    return TensorDataset(x, torch.from_numpy(y.astype(np.int64)))


class Net(nn.Module):
    def __init__(self, C, c1, c2, h, K):
        super().__init__()
        self.features = nn.Sequential(
            nn.Conv2d(C, c1, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
            nn.Conv2d(c1, c2, 3, padding=1), nn.ReLU(), nn.MaxPool2d(2),
        )
        self.head = nn.Sequential(
            nn.Flatten(), nn.Linear(7 * 7 * c2, h), nn.ReLU(),
            nn.Linear(h, K),
        )

    def forward(self, x):
        return self.head(self.features(x))


def n_params_formula(C, c1, c2, h, K):
    return 9 * C * c1 + c1 + 9 * c1 * c2 + c2 + 49 * c2 * h + h + h * K + K


def build_model():
    name, K, C = DATASETS[DATASET]
    c1, c2, h = WIDTHS[WIDTH]
    torch.manual_seed(0)
    model = Net(C, c1, c2, h, K)
    n_params = sum(p.numel() for p in model.parameters())
    assert n_params == n_params_formula(C, c1, c2, h, K)
    return model


def data():
    name, _, _ = DATASETS[DATASET]
    path = os.path.join(ROOT, "data", f"{name}.npz")
    return load_split(path, "train", n=N_TRAIN), load_split(path, "test")
