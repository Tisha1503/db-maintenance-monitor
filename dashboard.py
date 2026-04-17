from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from analytics import generate_report


def build_dashboard(db_path="monitoring.db", output_path="index.html"):
    report = generate_report(db_path)

    templates_dir = Path(__file__).parent / "templates"
    env = Environment(loader=FileSystemLoader(templates_dir))
    template = env.get_template("dashboard.html")

    html = template.render(data=report)

    with open(output_path, "w") as f:
        f.write(html)

    print(f"Dashboard generated: {output_path}")
    print("Open it in your browser to view.")


if __name__ == "__main__":
    build_dashboard()