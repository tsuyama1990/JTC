import subprocess
from pathlib import Path


def main():
    p = Path('dev_documents')
    p.mkdir(parents=True, exist_ok=True)
    res = subprocess.run(['uv', 'run', 'pytest', '--cov=src', '--cov-report=term-missing'], capture_output=True, text=True)
    (p / 'test_execution_log.txt').write_text(res.stdout + res.stderr)
    print(f'✓ Log saved: {p / "test_execution_log.txt"}')
    print(res.stdout)
    print(res.stderr)

if __name__ == "__main__":
    main()
