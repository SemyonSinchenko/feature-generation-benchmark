import json
from pathlib import Path

import jinja2

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

    for key in results_tiny:
        if results_tiny[key]["total_time"] == -1:
            results_tiny[key]["total_time"] = "OOM"
        else:
            results_tiny[key]["total_time"] = f"{results_tiny[key]['total_time']:.2f}"

    for key in results_small:
        if results_small[key]["total_time"] == -1:
            results_small[key]["total_time"] = "OOM"
        else:
            results_small[key]["total_time"] = f"{results_small[key]['total_time']:.2f}"

    for key in results_medium:
        if results_medium[key]["total_time"] == -1:
            results_medium[key]["total_time"] = "OOM"
        else:
            results_medium[key]["total_time"] = f"{results_medium[key]['total_time']:.2f}"

    with proj_root.joinpath("docs").joinpath("benchmark_results.md").open("w") as file_:
        filled = template.render(
            results_tiny=results_tiny,
            results_small=results_small,
            results_medium=results_medium,
        )
        file_.write(filled)
