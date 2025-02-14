import subprocess
import shutil
import os
from pathlib import Path
import configparser



def install_build_dependencies():
    """Install required build dependencies"""
    print("Installing build dependencies...")
    dependencies = [
        "build",
        "twine",
        "wheel",
        "setuptools>=42"
    ]
    for dep in dependencies:
        try:
            subprocess.run(["pip", "install", "--upgrade", dep], check=True)
            print(f"Installed {dep}")
        except subprocess.CalledProcessError as e:
            print(f"Error installing {dep}: {e}")
            raise


def setup_pypi_config():
    """Setup PyPI configuration using environment variables"""
    config = configparser.ConfigParser()
    
    # Basic distutils configuration
    config['distutils'] = {
        'index-servers': 'pypi testpypi'
    }
    
    # Production PyPI configuration
    config['pypi'] = {
        'username': '__token__',
        'password': os.environ.get('PYPI_TOKEN')
    }
    
 
    
    # Write to user's home directory
    pypi_rc = Path.home() / '.pypirc'
    with open(pypi_rc, 'w') as f:
        config.write(f)
    
    # Secure the file
    pypi_rc.chmod(0o600)
    
    return pypi_rc

def clean_build_dirs():
    """Clean up build directories"""
    dirs_to_clean = ['build', 'dist', 'delta_lens.egg-info']
    for dir_name in dirs_to_clean:
        if os.path.exists(dir_name):
            shutil.rmtree(dir_name)
            print(f"Cleaned {dir_name}/")

def build_package():
    """Build the package"""
    print("Building package...")
    subprocess.run(["python", "-m", "build"], check=True)

def check_package():
    """Check the built package with twine"""
    print("Checking package...")
    subprocess.run(["twine", "check", "dist/*"], check=True)

def publish_package():
    """Publish to PyPI"""
   
    if not os.environ.get('PYPI_TOKEN'):
        raise ValueError("PYPI_TOKEN environment variable not set")
    
    print("Publishing to PyPI...")
    repository = 'pypi'
    
    # Create PyPI config
    pypi_rc = setup_pypi_config()
    
    try:
        subprocess.run(["twine", "upload", f"--repository", repository, "dist/*"], check=True)
    finally:
        # Clean up credentials file
        pypi_rc.unlink()

if __name__ == "__main__":

    install_build_dependencies()

    # Clean, build, check
    clean_build_dirs()
    build_package()
    check_package()
    


    publish_package()
    print("Package published to PyPI successfully!")
