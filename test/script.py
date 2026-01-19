from pathlib import Path

current_directory = Path.cwd()
current_file = Path(__file__).name


print(f"files in {current_directory}: ")

for filepath in current_directory.iterdir():
    if filepath.name == current_file:
        continue

    print(f"- {filepath.name}")
    
    if filepath.is_file():
        content = filepath.read_text("utf-8")
        print(f" content: {content}")