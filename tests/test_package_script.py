from pathlib import Path


PACKAGE_SCRIPT = Path(__file__).resolve().parents[1] / 'package.sh'


def test_toolchain_downloads_require_verified_https():
    script = PACKAGE_SCRIPT.read_text(encoding='utf-8')
    download_commands = [
        line.strip() for line in script.splitlines()
        if line.strip().startswith('wget ')
    ]

    assert len(download_commands) == 2
    assert '--no-check-certificate' not in script
    assert all('--https-only' in command for command in download_commands)
    assert all('https://' in command for command in download_commands)
