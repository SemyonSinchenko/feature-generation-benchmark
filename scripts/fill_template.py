import json
from pathlib import Path

import jinja2
import matplotlib.pyplot as plt

RESULTS_TINY = "results_tiny.json"
RESULTS_SMALL = "results_small.json"
RESULTS_MEDIUM = "results_medium.json"


if __name__ == "__main__":
    proj_root = Path(__file__).parent.parent
    j_loader = jinja2.FileSystemLoader(searchpath=proj_root.joinpath("docs"))
    j_env = jinja2.Environment(loader=j_loader)
    template = j_env.get_template("benchmark_results.md.jinja2")

    results_tiny = json.load(proj_root.joinpath("results").joinpath(RESULTS_TINY).open("r"))
    results_small = json.load(proj_root.joinpath("results").joinpath(RESULTS_SMALL).open("r"))
    results_medium = json.load(proj_root.joinpath("results").joinpath(RESULTS_MEDIUM).open("r"))

    def clean_ooms(dct):
        for k in dct:
            if dct[k] == -1:
                dct[k] = 0

    clean_ooms(results_tiny)
    clean_ooms(results_small)
    clean_ooms(results_medium)

    images_prefix = proj_root.joinpath("docs").joinpath("static").absolute().__str__()
    f, ax = plt.subplots(nrows=3, ncols=1, figsize=(12, 12))

    f.suptitle("Time processing in seconds, less are better")

    y_pos = list(range(len(results_tiny)))
    ax[0].set_title("17M of rows")
    ax[0].barh(y_pos, [v for _, v in results_tiny.items()], align="center")
    ax[0].set_yticks(y_pos, results_tiny.keys())
    for i, kk in enumerate(results_medium):
        if results_tiny[kk] == 0:
            # Inspired by https://stackoverflow.com/a/30229062
            ax[0].text(100, i, "Memory error or no space left on device", color="red", verticalalignment="center")

    y_pos = list(range(len(results_small)))
    ax[1].set_title("170M of rows")
    ax[1].barh(y_pos, [v for _, v in results_small.items()], align="center")
    ax[1].set_yticks(y_pos, results_small.keys())
    for i, kk in enumerate(results_medium):
        if results_small[kk] == 0:
            # Inspired by https://stackoverflow.com/a/30229062
            ax[1].text(100, i, "Memory error or no space left on device", color="red", verticalalignment="center")

    y_pos = list(range(len(results_medium)))
    ax[2].set_title("1.7B of rows")
    ax[2].barh(y_pos, [v for _, v in results_medium.items()], align="center")
    ax[2].set_yticks(y_pos, results_medium.keys())
    for i, kk in enumerate(results_medium):
        if results_medium[kk] == 0:
            # Inspired by https://stackoverflow.com/a/30229062
            ax[2].text(
                100,
                i,
                "Memory error or no space left on device",
                color="red",
                verticalalignment="center",
            )

    f.tight_layout()

    f.savefig(f"{images_prefix}/results_overview.png")

    for key in results_tiny:
        if results_tiny[key] == 0:
            results_tiny[key] = "OOM"
        else:
            results_tiny[key] = f"{results_tiny[key]:.2f}"

    for key in results_small:
        if results_small[key] == 0:
            results_small[key] = "OOM"
        else:
            results_small[key] = f"{results_small[key]:.2f}"

    for key in results_medium:
        if results_medium[key] == 0:
            results_medium[key] = "OOM"
        else:
            results_medium[key] = f"{results_medium[key]:.2f}"

    with proj_root.joinpath("docs").joinpath("benchmark_results.md").open("w") as file_:
        filled = template.render(
            results_tiny=results_tiny,
            results_small=results_small,
            results_medium=results_medium,
        )
        file_.write(filled)
