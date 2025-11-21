#!/usr/bin/env python3

import numpy as np
import os
import sys
from tqdm import tqdm         
from get_sepsis_score import load_sepsis_model, get_sepsis_score


def load_challenge_data(filepath):
    with open(filepath, 'r') as f:
        header = f.readline().strip().split('|')
        lines = f.readlines()

    data_with_label = np.loadtxt(lines, delimiter='|')

    if header[-1] == "SepsisLabel":
        data = data_with_label[:, :-1]      # features
        true_labels = data_with_label[:, -1]  # SepsisLabel
    else:
        data = data_with_label
        true_labels = None 

    return data, true_labels



def main(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)

    model = load_sepsis_model()

    files = sorted([
        f for f in os.listdir(input_dir)
        if f.lower().endswith(".psv") and not f.startswith(".")
    ])

    for fname in tqdm(files, desc="Processing files", ncols=100):
        filepath = os.path.join(input_dir, fname)
        data, true_labels = load_challenge_data(filepath)  

        num_rows = len(data)
        scores = np.zeros(num_rows)
        labels = np.zeros(num_rows)

        for t in range(num_rows):
            prob, label = get_sepsis_score(data[:t+1], model)
            scores[t] = prob
            labels[t] = label

        output_path = os.path.join(output_dir, fname)
        with open(output_path, 'w') as f:
            if true_labels is not None:
                f.write("PredictedProbability|PredictedLabel|TrueLabel\n")
                for s, l, tl in zip(scores, labels, true_labels):
                    f.write(f"{s}|{int(l)}|{int(tl)}\n")
            else:
                f.write("PredictedProbability|PredictedLabel\n")
                for s, l in zip(scores, labels):
                    f.write(f"{s}|{int(l)}\n")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python driver.py <input_dir> <output_dir>")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])