import subprocess
import pytest
import shutil

def test_bandit_is_executable():
    """Check if bandit is available in the system path."""
    assert shutil.which("bandit") is not None, "Bandit executable not found in PATH"

def test_pip_audit_is_executable():
    """Check if pip-audit is available in the system path."""
    assert shutil.which("pip-audit") is not None, "Pip-audit executable not found in PATH"

def test_requirements_file_has_security_tools():
    """Ensure requirements.txt includes the required security tools."""
    with open("requirements.txt", "r") as f:
        content = f.read()
        assert "bandit" in content.lower()
        assert "pip-audit" in content.lower()
